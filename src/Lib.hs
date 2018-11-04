{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib
  ( Config(..)
  , defaultDelimiter
  , defaultBufferSize
  , app
  , probe
  ) where

import qualified System.IO as IO
import qualified Control.Exception as Exception
import qualified Data.Maybe as Maybe
import           Data.List.NonEmpty (NonEmpty)
import           Data.Char (ord)
import           Data.Function (on)

import qualified Data.Time as Time
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Csv as Csv
import qualified Data.Csv.Incremental as Csvi
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Streamly ((|$))
import qualified Streamly as S
import qualified Streamly.Prelude as S
import qualified System.IO.Temp as Temp
import qualified System.Directory as Dir

defaultDelimiter :: Char
defaultDelimiter = ','

defaultBufferSize :: BufferSize
defaultBufferSize = 200 * 1024 * 1024

type RowId = Int
type BufferSize = Int
type Row = Csv.Record
type PKIdx = NonEmpty Int
type PK = NonEmpty Csv.Field
type Delim = Char

data Entity a
  = Entity
  { entityKey :: PK
  , entityVal :: a
  }
  deriving Show
instance Eq (Entity a) where
  (==) = (==) `on` entityKey
instance Ord (Entity a) where
  (<=) = (<=) `on` entityKey

data Config
  = Config
  { configSource :: FilePath
  , configHasHeader :: Bool
  , configDelimiter :: Delim
  , configBufferSize :: BufferSize
  , configKeys :: PKIdx
  , configDestination :: Maybe FilePath
  } deriving Show

data Error
  = SrcFileNotFound FilePath
  | LineParseError RowId String
  | FileParseError RowId BS.ByteString String
  | RowPrimaryKeyOutOfBounds RowId
  deriving Show
instance Exception.Exception Error

debug :: Bool
debug = False

app :: Config -> IO ()
app c = IO.withFile (configSource c) IO.ReadMode (go (configHasHeader c))
  where
    withTempDir False m = Temp.withSystemTempDirectory "filesort.txt" m
    withTempDir True m = m ".scratch/debug"
    go hasHeader' h = do
      let hasHeader = if hasHeader' then Csv.HasHeader else Csv.NoHeader
      header <- getHeader hasHeader h
      withTempDir debug $ \dir -> do
        createSortedSlices c hasHeader h dir
        mergeSortedSlices c header dir

{-# INLINABLE createSortedSlices #-}
createSortedSlices :: Config -> Csv.HasHeader -> IO.Handle -> FilePath -> IO ()
createSortedSlices c hasHeader h dir
  = S.runStream
  -- . S.asyncly
  $ S.mapM (\rs -> VA.sort rs >> V.unsafeFreeze rs >>= saveTemp delim dir . fmap entityVal)
  |$ S.mapM (V.unsafeThaw . V.fromList)
  . fmap (fmap (uncurry Entity . validate pkIdx))
  -- |$ probe (putStrLn . (++) "fromCSV row#" . show . fst)
  . fromCsv delim (configBufferSize c) rowId
  $ h
  where
    pkIdx = configKeys c
    delim = configDelimiter c
    rowId = startRowId hasHeader
    -- sortByPk = VA.sortBy (compare `on` fst)

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
    delim = configDelimiter c
    go h = do
      Maybe.maybe (pure ()) (hPutLnBS h) header
      -- N way merge; we assume number of files = N
      -- TODO: perform multiple N way merges to handle large N
      fps <- listDirectory dir
      handles <- mapM (`IO.openFile` IO.ReadMode) fps
      let fromCsv' = fmap (uncurry Entity . validate (configKeys c)) . flatten . fromCsv delim (configBufferSize c) 0
      let streams = fmap fromCsv' handles
      S.runStream
        -- . S.aheadly
        . S.mapM (LBS.hPut h)
        . fmap (\x -> Csv.encodeWith (csvEncodeOpt delim) [entityVal x])
        $ mergeNAsync streams
      mapM_ IO.hClose handles

-- mergeNAsync :: [S.Serial Row] -> S.Serial Row
-- mergeNAsync ss = mapM (S.yieldM . S.mkAsync) ss >>= foldr merge2 S.nil

mergeNAsync :: Ord a => [S.Serial a] -> S.Serial a
mergeNAsync [] = S.nil
mergeNAsync [x] = x
mergeNAsync [x1,x2] = merge2Async x1 x2
mergeNAsync xs
  = mergeNAsync
  . fmap (uncurry merge2Async)
  $ zip left right
  where
    (left, right) = equalHalves xs

equalHalves :: Monoid a => [a] -> ([a], [a])
equalHalves xs = (left <> l, right)
  where
    n = length xs `div` 2
    left = take n xs
    right = drop n xs
    l = [mempty | odd (length xs)]

-- | merge two streams generating the elements from each in parallel
merge2Async :: Ord a => S.Serial a -> S.Serial a -> S.Serial a
merge2Async a b = do
    x <- S.yieldM $ S.mkAsync a
    y <- S.yieldM $ S.mkAsync b
    merge2 x y

merge2 :: Ord a => S.Serial a -> S.Serial a -> S.Serial a
merge2 a b = do
  a1 <- S.yieldM $ S.uncons a
  case a1 of
    Nothing -> b
    Just (x, ma) -> do
      b1 <- S.yieldM $ S.uncons b
      case b1 of
        Nothing -> return x <> ma
        Just (y, mb) ->
          if y < x
          then return y <> merge2 (return x <> ma) mb
          else return x <> merge2 ma (return y <> mb)

flatten :: S.MonadAsync m => S.SerialT m [a] -> S.SerialT m a
flatten a = do
  a1 <- S.yieldM $ S.uncons a
  case a1 of
    Nothing -> S.nil
    Just (x, ma) -> S.fromList x <> flatten ma

listDirectory :: FilePath -> IO [FilePath]
listDirectory dir = fmap go <$> Dir.listDirectory dir
  where
    go fp = dir <> "/" <> fp

getTempFile :: FilePath -> IO FilePath
getTempFile dir = (\fp -> dir ++ "/" ++ fp) . format <$> Time.getCurrentTime
  where
    format = Time.formatTime Time.defaultTimeLocale "%s.%q"

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
  = step (initRowId, Csvi.decodeWith opt Csv.NoHeader)
  where
    opt = Csv.DecodeOptions (fromIntegral . ord $ delim)
    step (rowId, Csvi.Fail bs e)  = Exception.throw $ FileParseError rowId bs e
    step (rowId, Csvi.Many rs k)  = consume rowId rs S..: next
      where
        next = do
          p <- liftIO $ feed k
          step (rowId+length rs, p)
    step (rowId, Csvi.Done rs)    = S.yield (consume rowId rs)
    consume rowId = zip [rowId..]
    feed k = do
      isEof <- IO.hIsEOF h
      if isEof
        then return $ k mempty
        else k `fmap` BS.hGet h bSize

getPK :: PKIdx -> Row -> Maybe PK
getPK pkIdx v = mapM ((V.!?) v) pkIdx
