module Main where

import           Control.Applicative (optional)
import qualified Lib
import qualified Data.List.NonEmpty as NonEmpty
import qualified Options.Applicative as Opt

main :: IO ()
main = Lib.app =<< Opt.execParser opts
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
     <> Opt.progDesc "Sort SOURCE by KEYS into DESTINATION"
     <> Opt.header "filesort - RFC 4180 compliant sorting utility" )

config :: Opt.Parser Lib.Config
config
  = Lib.Config
  <$> Opt.strOption
      ( Opt.long "in"
      <> Opt.metavar "SOURCE"
      <> Opt.help "Source FilePath" )
  <*> Opt.switch
      ( Opt.long "header"
      <> Opt.help "Whether file has header" )
  <*> Opt.option Opt.auto
      ( Opt.long "delimiter"
      <> Opt.help "Field separator"
      <> Opt.showDefault
      <> Opt.value Lib.defaultDelimiter )
  <*> Opt.option Opt.auto
      ( Opt.long "buffer_size"
      <> Opt.help "Bytes to stream in at once"
      <> Opt.showDefault
      <> Opt.value Lib.defaultBufferSize )
  <*> Opt.option (Opt.maybeReader parseNonEmpty)
      ( Opt.long "keys"
      <> Opt.metavar "KEYS"
      <> Opt.help "Indicies of the fields to sort by" )
  <*> optional (Opt.strOption
      ( Opt.long "out"
      <> Opt.metavar "DESTINATION"
      <> Opt.help "Where to output results; defaults to STDOUT" ))

parseNonEmpty :: (Read a) => String -> Maybe (NonEmpty.NonEmpty a)
parseNonEmpty s = readMaybe s >>= NonEmpty.nonEmpty

readMaybe :: (Read a) => String -> Maybe a
readMaybe s = case reads s of
              [(x, "")] -> Just x
              _ -> Nothing
