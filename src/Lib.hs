{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib (
    Config(..)
  , defaultDelimiter
  , defaultBufferSize
  , app
) where

import qualified System.IO as IO
import qualified Control.Exception as Exception
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import qualified Data.List.NonEmpty as NonEmpty
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
import           Control.Monad.IO.Class (liftIO)
import qualified Streamly as S
import qualified Streamly.Prelude as S
import qualified System.IO.Temp as Temp
import qualified System.Directory as Dir

defaultDelimiter :: Char
defaultDelimiter = ','

defaultBufferSize :: Int
defaultBufferSize = 200 * 1024 * 1024

type RowId = Int
type BufferSize = Int
type Row = Csv.Record
type PKIdx = NonEmpty Int
type Delim = Char

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
    pkIdx = configKeys c
    withTempDir False m = Temp.withSystemTempDirectory "filesort.txt" m
    withTempDir True m = m ".scratch/base"
    go hasHeader' h = do
      let hasHeader = if hasHeader' then Csv.HasHeader else Csv.NoHeader
      header <- getHeader hasHeader h
      withTempDir debug $ \dir -> do
        toSortedSlices hasHeader h dir
        mergeSlices (configDelimiter c) header dir (configDestination c)
    toSortedSlices hasHeader h dir
      = S.runStream
      . S.aheadly
      . S.mapM (\rs -> sortByPk rs >> V.unsafeFreeze rs >>= saveTemp)
      . S.mapM (V.unsafeThaw . V.fromList)
      . fromCsv (configDelimiter c) (configBufferSize c) pkIdx rowId
      $ h
      where
        rowId = startRowId hasHeader
        sortByPk = VA.sortBy (compare `on` getPK pkIdx)
        saveTemp rs = getTempFile dir >>= flip LBS.writeFile bs
          where
            rs' = foldMap Csvi.encodeRecord rs
            bs = Csvi.encodeWith (csvEncodeOpt (configDelimiter c)) rs'

hPutLnBS :: IO.Handle -> BS.ByteString -> IO ()
hPutLnBS h bs = BS.hPut h (bs <> "\n")

{-# INLINE mergeSlices #-}
mergeSlices :: Char -> Maybe BS.ByteString -> FilePath -> Maybe FilePath -> IO ()
mergeSlices _delim header dir target = withTarget target go
  where
    withTarget (Just fp) a = IO.withFile fp IO.WriteMode a
    withTarget Nothing a = a IO.stdout
    go h = do
      Maybe.maybe (pure ()) (hPutLnBS h) header
      fps <- listDirectory dir
      let fp = head . tail $ fps
      IO.withFile fp IO.ReadMode $ \rh ->
          S.runStream
        . S.serially
        . S.mapM (hPutLnBS h)
        $ fromHandleBS rh

listDirectory :: FilePath -> IO [FilePath]
listDirectory dir = fmap go <$> Dir.listDirectory dir
  where
    go fp = dir <> "/" <> fp

getTempFile :: FilePath -> IO FilePath
getTempFile dir = (\fp -> dir ++ "/" ++ fp) . format <$> Time.getCurrentTime
  where
    format = Time.formatTime Time.defaultTimeLocale "%s.%q"

csvEncodeOpt :: Char -> Csv.EncodeOptions
csvEncodeOpt delim = Csv.defaultEncodeOptions { Csv.encDelimiter = delim' }
  where
    delim' = fromIntegral . ord $ delim

startRowId :: Csv.HasHeader -> Int
startRowId Csv.NoHeader = 0
startRowId Csv.HasHeader = 1

getHeader :: Csv.HasHeader -> IO.Handle -> IO (Maybe BS.ByteString)
getHeader Csv.NoHeader _ = pure Nothing
getHeader Csv.HasHeader h = pure <$> BS.hGetLine h

fromHandleBS :: (S.IsStream t, S.MonadAsync m) => IO.Handle -> t m BS.ByteString
fromHandleBS = S.unfoldrM (liftIO . go)
  where
    go h = do
      isEof <- IO.hIsEOF h
      if isEof
        then return Nothing
        else do
          l <- BS.hGetLine h
          return . pure $ (l, h)

{-# INLINE fromCsv #-}
fromCsv
  :: (S.IsStream t, S.MonadAsync m)
  => Delim -> BufferSize -> PKIdx -> Int -> IO.Handle -> t m [Row]
fromCsv delim bSize pkIdx initRowId h
  = S.unfoldrM (liftIO . step) (initRowId, Csvi.decodeWith opt Csv.NoHeader)
  where
    opt = Csv.DecodeOptions (fromIntegral . ord $ delim)
    edgeIdx = List.minimum pkIdx NonEmpty.:| [List.maximum pkIdx]
    step (rowId, Csvi.Fail bs e)  = Exception.throw $ FileParseError rowId bs e
    step (rowId, Csvi.Many rs k)  = consume edgeIdx rowId rs <$> feed k
    step (rowId, Csvi.Done rs)    = pure $ consume edgeIdx rowId rs (Csvi.Done [])
    feed k = do
      isEof <- IO.hIsEOF h
      if isEof
        then return $ k mempty
        else k `fmap` BS.hGet h bSize

getPK :: PKIdx -> Row -> Maybe (NonEmpty Csv.Field)
getPK pkIdx v = mapM ((V.!?) v) pkIdx

consume
  :: PKIdx
  -> RowId
  -> [Either String Row]
  -> Csvi.Parser Row
  -> Maybe ([Row], (RowId, Csvi.Parser Row))
consume _ _ [] (Csvi.Done []) = Nothing
consume edgeIdx rowId' rs next = pure (rows rowId', (rowId' + length rs, next))
  where
    parse rowId row
      = if Maybe.isJust (getPK edgeIdx row)
        then row
        else Exception.throw $ RowPrimaryKeyOutOfBounds rowId
    guard (rowId, Left e) = Exception.throw $ LineParseError rowId e
    guard (rowId, Right row) = parse rowId row
    rows rowId = guard <$> zip [rowId..] rs
