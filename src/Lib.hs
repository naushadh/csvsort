{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module Lib
  ( Config(..)
  , defaultDelimiter
  , defaultBufferSize
  , mkBufferSize
  , BufferSize
  , defaultNMerge
  , mkNMerge
  , NMerge
  , app
  , probe
  , Pos
  , mkPos
  ) where

import qualified System.IO as IO
import qualified Control.Exception as Exception
import qualified Data.Maybe as Maybe
import           Data.List.NonEmpty (NonEmpty)
import           Data.Char (ord)
import           Data.Function (on)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Vector as V
import qualified Data.Vector.Split as Split
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Csv as Csv
import qualified Data.Csv.Incremental as Csvi
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Streamly ((|$))
import qualified Streamly as S
import qualified Streamly.Prelude as S
import qualified System.IO.Temp as Temp
import qualified System.Directory as Dir
import qualified System.FilePath as FP
import qualified Data.PQueue.Min as Q

import qualified Data.Time as Time

-- logging
-- import qualified Data.Time as Time
-- import qualified Data.Text as T
-- import qualified Data.Text.IO as TIO

defaultDelimiter :: Char
defaultDelimiter = ','

data ByteMultiplier = B | KB | MB | GB | TB | PB | EB | ZB | YB
  deriving Show

mkByteMultiplier :: String -> Either String ByteMultiplier
mkByteMultiplier "b" = pure B
mkByteMultiplier ""  = pure KB
mkByteMultiplier "K" = pure KB
mkByteMultiplier "M" = pure MB
mkByteMultiplier "G" = pure GB
mkByteMultiplier "T" = pure TB
mkByteMultiplier "P" = pure PB
mkByteMultiplier "E" = pure EB
mkByteMultiplier "Z" = pure ZB
mkByteMultiplier "Y" = pure YB
mkByteMultiplier _   = Left "Invalid multiplier"

intByteMultiplier :: ByteMultiplier -> Int
intByteMultiplier  B = 1024^(0::Int)
intByteMultiplier KB = 1024^(1::Int)
intByteMultiplier MB = 1024^(2::Int)
intByteMultiplier GB = 1024^(3::Int)
intByteMultiplier TB = 1024^(4::Int)
intByteMultiplier PB = 1024^(5::Int)
intByteMultiplier EB = 1024^(6::Int)
intByteMultiplier ZB = 1024^(7::Int)
intByteMultiplier YB = 1024^(8::Int)

data BufferSize = BufferSize ByteMultiplier Int
  deriving Show

getBufferSize :: BufferSize -> Int
getBufferSize (BufferSize m n) = (intByteMultiplier m * n) `div` divFactor
  where
    -- this magic ratio seems to work best
    -- TODO: find a better way to calculate this based on input row(s)
    divFactor = 10 * 1024

defaultBufferSize :: BufferSize
defaultBufferSize = BufferSize MB 200

mkBufferSize :: String -> Either String BufferSize
mkBufferSize = go . reads
  where
    go [(n, m)] = BufferSize <$> mkByteMultiplier m <*> parseVal n
    go _ = Left "Invalid multiplier or value"
    parseVal n
      | n < min' = Left $ "Must at-least be: " <> show min'
      | otherwise = pure n
      where min' = 10 * 1024

type RowId = Int
type Row = Csv.Record
type PKIdx = NonEmpty Pos
type PK = NonEmpty Csv.Field
type Delim = Char

newtype Pos = Pos { unPos :: Int }
instance Show Pos where
  show = show . unPos

mkPos :: Int -> Either String Pos
mkPos n
  | n < min' = Left $ "Must at-least be: " <> show min'
  | otherwise= pure . Pos . (-) 1 $ n
  where
    min' = 1

newtype NMerge = NMerge { getNMerge :: Int }
instance Show NMerge where
  show = show . getNMerge

defaultNMerge :: NMerge
defaultNMerge = NMerge 16

mkNMerge :: Int -> Either String NMerge
mkNMerge n
  | n < min' = Left $ "Must at-least be: " <> show min'
  | n > max' = Left $ "Cannot exceed rlimit: " <> show max'
  | otherwise    = pure $ NMerge n
  where
    min' = 2
    max' = 20 -- TODO: constrain by RLIMIT

data Entity a
  = Entity
  { entityKey :: PK
  , entityVal :: a
  } deriving Show
instance Eq (Entity a) where
  (==) = (==) `on` entityKey
instance Ord (Entity a) where
  (<=) = (<=) `on` entityKey

data Indexed a
  = Indexed
  { indexKey :: Int
  , indexVal :: a
  } deriving Show
instance Eq a => Eq (Indexed a) where
  (==) = (==) `on` indexVal
instance Ord a => Ord (Indexed a) where
  (<=) = (<=) `on` indexVal

data Config
  = Config
  { configSource :: FilePath
  , configHasHeader :: Bool
  , configDelimiter :: Delim
  , configBufferSize :: BufferSize
  , configBatchSize :: NMerge
  , configKeys :: PKIdx
  , configDestination :: Maybe FilePath
  , configParallel :: Int
  } deriving Show

data Error
  = SrcFileNotFound FilePath
  | LineParseError RowId String
  | FileParseError RowId BS.ByteString String
  | RowPrimaryKeyOutOfBounds RowId
  deriving Show
instance Exception.Exception Error

debug :: Bool
debug = True

-- logStdout :: T.Text -> IO ()
-- logStdout msg = Time.getCurrentTime >>= TIO.putStrLn . tpack >> TIO.putStrLn msg
--   where
--     tpack :: Show a => a -> T.Text
--     tpack = T.pack . show

app :: Config -> IO ()
app c = IO.withFile (configSource c) IO.ReadMode (go (configHasHeader c))
  where
    withTempDir False m = Temp.withSystemTempDirectory "filesort.txt" m
    withTempDir True m = m ".scratch/debug"
    go hasHeader' h = do
      let hasHeader = if hasHeader' then Csv.HasHeader else Csv.NoHeader
      header <- getHeader hasHeader h
      withTempDir debug $ \dir -> do
        -- logStdout "split: BEGIN"
        createSortedSlices c hasHeader h dir
        -- logStdout "split: END; merge: BEGIN"
        mergeSortedSlices c header dir
        -- logStdout "merge: END"

{-# INLINABLE createSortedSlices #-}
createSortedSlices :: Config -> Csv.HasHeader -> IO.Handle -> FilePath -> IO ()
createSortedSlices c hasHeader h dir
  = S.runStream
  . S.asyncly
  $ S.mapM (saveTemp delim dir . fmap entityVal)
  |$ S.mapM V.unsafeFreeze
  |$ S.mapM (\rs -> VA.sort rs >> pure rs)
  |$ S.mapM V.unsafeThaw
  $ S.map V.fromList
  -- |$ S.mapM sort'
  . S.filter (not . null)
  . fmap (fmap (uncurry Entity . validate pkIdx))
  . fromCsv delim (configBufferSize c) rowId
  $ h
  where
    pkIdx = configKeys c
    delim = configDelimiter c
    rowId = startRowId hasHeader
    -- internals are unsafe; trying to keep dangerous bits in one place.
    -- -- {-# INLINE sort' #-}
    -- sort' xs = do
    --   mxs <- V.unsafeThaw . V.fromList $ xs
    --   VA.sort mxs
    --   V.unsafeFreeze mxs

saveTemp :: Char -> FilePath -> V.Vector Row -> IO ()
saveTemp delim dir rs = getTempFile dir >>= flip LBS.writeFile bs -- >> putStrLn "Saved to Temp"
  where
    rs' = foldMap Csvi.encodeRecord rs
    bs = Csvi.encodeWith (csvEncodeOpt delim) rs'

probe :: (S.IsStream t, S.MonadAsync m) => (a -> IO ()) -> t m a -> t m a
probe m = S.mapM (\x -> liftIO (m x) >> pure x)

hPutLnBS :: IO.Handle -> BS.ByteString -> IO ()
hPutLnBS h bs = BS.hPut h (bs <> "\n")

{-# INLINABLE mergeSortedSlices #-}
mergeSortedSlices :: Config -> Maybe BS.ByteString -> FilePath -> IO ()
mergeSortedSlices c header dir = withTarget (configDestination c) go
  where
    withTarget (Just fp) a = IO.withFile fp IO.WriteMode a
    withTarget Nothing a = a IO.stdout
    go h = do
      Maybe.maybe (pure ()) (hPutLnBS h) header
      fps <- listDirectory dir
      fpSorted <- mergeNFiles c (V.fromList fps)
      let toHandle fp = LBS.readFile fp >>= LBS.hPut h
      Maybe.maybe (pure ()) toHandle fpSorted

mergeNFiles :: Config -> V.Vector FilePath -> IO (Maybe FilePath)
mergeNFiles c fs
  | n == 0  = pure Nothing
  | n == 1  = pure . pure $ V.head fs
  | n <= mf = pure <$> mergeNFiles' c fs
  | otherwise = go >>= mergeNFiles c
  where
    mf = getNMerge . configBatchSize $ c
    n = V.length fs
    go = do
        let cfs = Split.chunksOf mf fs
        tStart <- Time.getCurrentTime
        S.mapM_ (mergeNFiles' c) $ S.fromList cfs
        tEnd <- Time.getCurrentTime
        print $ Time.diffUTCTime tEnd tStart
        pure . V.fromList $ fmap V.head cfs

mergeNFiles' :: Config -> V.Vector FilePath -> IO FilePath
mergeNFiles' c fs = do
  let fp1 = V.head fs
  let dir = FP.takeDirectory fp1
  fpN <- getTempFile dir
  let delim = configDelimiter c
  let flatten = S.concatMap id :: S.Serial (S.Serial a) -> S.Serial a
  let fromCsv'
        = fmap (uncurry Entity . validate (configKeys c))
        . flatten
        . S.map S.fromList
        . fromCsv delim (configBufferSize c) 0
  let go hs = do
        let ss = fmap fromCsv' hs
        let sN = mergeNAsync ss
        toCsv delim fpN . S.map entityVal $ sN

  Exception.bracket
    (mapM (`IO.openFile` IO.ReadMode) fs)
    (mapM_ IO.hClose)
    go

  -- we overwrite N to 1 since `Streamly`
  -- - discards results on `runStream` (which supports async processing)
  -- - returns results in `toList` (which only supports serial processing)
  -- so moving to fp1 allows the caller to indirectly collect the merged result
  mapM_ Dir.removeFile fs
  Dir.renameFile fpN fp1
  return fp1

mergeNAsync :: Ord a => V.Vector (S.Serial a) -> S.Serial a
mergeNAsync ss' = do
  ss <- mapM (S.yieldM . S.mkAsync) ss'
  mhs <- mapM (S.yieldM . S.uncons) ss
  let (hs,ssO) = V.unzip . fmap Maybe.fromJust . V.filter Maybe.isJust $ mhs
  let ihs = V.imap Indexed hs
  mergeN (Q.fromList . V.toList $ ihs) ssO

mergeN :: Ord a => Q.MinQueue (Indexed a) -> V.Vector (S.Serial a) -> S.Serial a
mergeN q ss
  | n == 0 = mempty
  | n == 1 = V.head ss
  | otherwise = go
  where
    n = V.length ss
    go = case Q.minView q of
          Nothing -> S.nil
          Just (Indexed i x, qRem) -> do
            let s = ss V.! i
            mh <- S.yieldM . S.uncons $ s
            let (qNext, oss)
                  = case mh of
                    Nothing -> (qRem, ss)
                    Just (h, sNext) -> (Q.insert (Indexed i h) qRem, modifyVectorIdx i sNext ss)
            x `S.cons` mergeN qNext oss

-- faster than `V.//` or `V.update`
modifyVectorIdx :: Int -> a -> V.Vector a -> V.Vector a
modifyVectorIdx i x = V.modify (\v -> VGM.write v i x)

listDirectory :: FilePath -> IO [FilePath]
listDirectory dir = fmap go <$> Dir.listDirectory dir
  where
    go fp = dir <> "/" <> fp

getTempFile :: FilePath -> IO FilePath
getTempFile dir = Temp.emptyTempFile dir "sort.txt"

csvEncodeOpt :: Char -> Csv.EncodeOptions
csvEncodeOpt delim
  = Csv.defaultEncodeOptions
  { Csv.encDelimiter = delim'
  , Csv.encUseCrLf = False
  }
  where
    delim' = fromIntegral . ord $ delim

startRowId :: Csv.HasHeader -> Int
startRowId Csv.NoHeader = 0
startRowId Csv.HasHeader = 1

getHeader :: Csv.HasHeader -> IO.Handle -> IO (Maybe BS.ByteString)
getHeader Csv.NoHeader _ = pure Nothing
getHeader Csv.HasHeader h = pure <$> BS.hGetLine h

validate :: PKIdx -> (RowId, Either String Row) -> (PK, Row)
validate _ (rowId, Left e) = Exception.throw $ LineParseError rowId e
validate pkIdx (rowId, Right r)
  = case getPK pkIdx r of
    (Just pk) -> (pk, r)
    Nothing -> Exception.throw $ RowPrimaryKeyOutOfBounds rowId

{-# INLINABLE fromCsv #-}
fromCsv
  ::( S.IsStream t
    , S.MonadAsync m
    , MonadIO (t m)
    , Csv.FromRecord a
    )
  => Delim -> BufferSize -> Int -> IO.Handle -> t m [(RowId, Either String a)]
fromCsv delim bSize initRowId h
  = S.unfoldrM (liftIO . produce) (initRowId, pure $ Csvi.decodeWith opt Csv.NoHeader)
  where
    opt = Csv.DecodeOptions (fromIntegral . ord $ delim)

    parseResult rowId (Csvi.Fail bs err) = Exception.throw $ FileParseError rowId bs err
    parseResult rowId (Csvi.Many rs k)   = (zip [rowId..] rs, pure k)
    parseResult rowId (Csvi.Done rs)     = (zip [rowId..] rs, Nothing)

    produce (!_, Nothing) = pure Nothing
    produce (rowId, Just parser) = do
      let (rs, mkNextParser) = parseResult rowId parser
      isEof <- IO.hIsEOF h
      bs <- if isEof
        then pure mempty
        else BS.hGet h $ getBufferSize bSize
      pure . pure $ (rs, (rowId+length rs, fmap (\p -> p bs) mkNextParser))

getPK :: PKIdx -> Row -> Maybe PK
getPK pkIdx v = mapM ((V.!?) v . unPos) pkIdx

{-# INLINABLE toCsv #-}
toCsv
  :: (Csv.ToRecord a, Show a)
  => Char
  -> FilePath
  -> S.SerialT IO a
  -> IO ()
toCsv delim fp s = IO.withFile fp IO.WriteMode go
  where
    go h= S.mapM_ (BS.hPut h)
        . S.map (LBS.toStrict . Csv.encodeWith (csvEncodeOpt delim) . pure)
        $ s
