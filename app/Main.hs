module Main where

import           Control.Applicative (optional)
import qualified Lib
import qualified Data.List.NonEmpty as NonEmpty
import qualified Options.Applicative as Opt
import           GHC.Conc (getNumCapabilities)

main :: IO ()
main = getNumCapabilities >>= Opt.execParser . opts >>= Lib.app
  where
    opts n = Opt.info (Opt.helper <*> config n)
      ( Opt.fullDesc
     <> Opt.progDesc "Sort SOURCE by KEYS into DESTINATION"
     <> Opt.header "filesort - RFC 4180 compliant sorting utility" )

config :: Int -> Opt.Parser Lib.Config
config numCores
  = Lib.Config
  <$> Opt.strOption
      ( Opt.long "in"
      <> Opt.metavar "SOURCE"
      <> Opt.help "Source FilePath" )
  <*> Opt.switch
      ( Opt.long "header"
      <> Opt.help "Whether file has header" )
  <*> Opt.option Opt.auto
      ( Opt.long "field-separator"
      <> Opt.help "Field separator"
      <> Opt.showDefault
      <> Opt.value Lib.defaultDelimiter )
  <*> Opt.option (Opt.maybeReader parseBufferSize)
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
  <*> Opt.option (Opt.maybeReader parseNonEmpty)
      ( Opt.long "keys"
      <> Opt.metavar "KEYS"
      <> Opt.help "Indicies of the fields to sort by" )
  <*> optional (Opt.strOption
      ( Opt.long "output"
      <> Opt.metavar "DESTINATION"
      <> Opt.help "Where to output results; defaults to STDOUT" ))
  <*> Opt.option Opt.auto
      ( Opt.long "parallel"
      <> Opt.metavar "N"
      <> Opt.help "change the number of sorts run concurrently to N"
      <> Opt.value numCores )

parseBufferSize :: String -> Maybe Int
parseBufferSize s = case reads s of
    [(x, "b")] -> pure x
    [(x, "")] ->  pure (x * 1024)
    [(x, "K")] -> pure (x * 1024)
    [(x, "M")] -> pure (x * 1024 * 1024)
    [(x, "G")] -> pure (x * 1024 * 1024 * 1024)
    [(x, "T")] -> pure (x * 1024 * 1024 * 1024 * 1024)
    [(x, "P")] -> pure (x * 1024 * 1024 * 1024 * 1024 * 1024)
    [(x, "E")] -> pure (x * 1024 * 1024 * 1024 * 1024 * 1024 * 1024)
    [(x, "Z")] -> pure (x * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024)
    [(x, "Y")] -> pure (x * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024)
    _ -> Nothing

parseNMerge :: String -> Either String Lib.NMerge
parseNMerge s = readEither "Int" s >>= Lib.mkNMerge

parseNonEmpty :: Read a => String -> Maybe (NonEmpty.NonEmpty a)
parseNonEmpty s = readMaybe s >>= NonEmpty.nonEmpty

readMaybe :: Read a => String -> Maybe a
readMaybe s = case reads s of
                [(x, "")] -> Just x
                _ -> Nothing

readEither :: Read a => String -> String -> Either String a
readEither t s = case reads s of
                [(x, "")] -> pure x
                _ -> Left $ "Expected " <> t
