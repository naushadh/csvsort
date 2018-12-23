module Main where

import           Control.Applicative (optional, some)
import qualified Lib
import qualified Data.List.NonEmpty as NonEmpty
import qualified Options.Applicative as Opt
import           GHC.Conc (getNumCapabilities, setNumCapabilities)

main :: IO ()
main = do
  nDefault <- getNumCapabilities
  c <- Opt.execParser . opts $ nDefault
  setNumCapabilities . Lib.configParallel $ c
  Lib.app c
  where
    opts n = Opt.info (Opt.helper <*> config n)
      ( Opt.fullDesc
     <> Opt.progDesc "Sort SOURCE by KEYS into DESTINATION"
     <> Opt.header "filesort - RFC 4180 compliant sorting utility" )

config :: Int -> Opt.Parser Lib.Config
config numCores
  = Lib.Config
  <$> Opt.strArgument
      (  Opt.metavar "SOURCE"
      <> Opt.help "Source FilePath" )
  <*> Opt.switch
      ( Opt.long "header"
      <> Opt.help "Whether file has header" )
  <*> Opt.option Opt.auto
      ( Opt.long "field-separator"
      <> Opt.help "Field separator"
      <> Opt.showDefault
      <> Opt.value Lib.defaultDelimiter )
  <*> Opt.option (Opt.eitherReader Lib.mkBufferSize)
      ( Opt.long "buffer-size"
      <> Opt.metavar "SIZE"
      <> Opt.help "Use SIZE for main memory buffer. SIZE may be followed by the \
                    \ following multiplicative suffixes: % 1% of memory, b 1, K \
                    \ 1024 (default), and so on for M, G, T, P, E, Z, Y."
      <> Opt.showDefault
      <> Opt.value Lib.defaultBufferSize )
  <*> Opt.option (Opt.eitherReader parseNMerge)
      ( Opt.long "batch-size"
      <> Opt.metavar "NMERGE"
      <> Opt.help "merge at most NMERGE inputs at once"
      <> Opt.showDefault
      <> Opt.value Lib.defaultNMerge )
  <*> fmap NonEmpty.fromList (some (Opt.option (Opt.eitherReader parsePos)
      ( Opt.long "key"
      <> Opt.metavar "POS"
      <> Opt.help "Position of a key (origin 0)" )))
  <*> optional (Opt.strOption
      ( Opt.long "output"
      <> Opt.metavar "DESTINATION"
      <> Opt.help "Where to output results; defaults to STDOUT" ))
  <*> Opt.option Opt.auto
      ( Opt.long "parallel"
      <> Opt.metavar "N"
      <> Opt.help "change the number of sorts run concurrently to N"
      <> Opt.value numCores )

parseNMerge :: String -> Either String Lib.NMerge
parseNMerge s = readEither "Int" s >>= Lib.mkNMerge

parsePos :: String -> Either String Lib.Pos
parsePos s = readEither "Pos" s >>= Lib.mkPos

readMaybe :: Read a => String -> Maybe a
readMaybe s = case reads s of
                [(x, "")] -> Just x
                _ -> Nothing

readEither :: Read a => String -> String -> Either String a
readEither t s = case reads s of
                [(x, "")] -> pure x
                _ -> Left $ "Expected " <> t
