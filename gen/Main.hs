{-# LANGUAGE FlexibleContexts #-}

module Main where

import           Numeric.Natural (Natural)
import qualified System.IO as IO

import qualified Streamly as S
import qualified Streamly.Prelude as S
import qualified Options.Applicative as Opt

main :: IO ()
main = app =<< Opt.execParser opts
  where
    opts = Opt.info (Opt.helper <*> config)
      ( Opt.fullDesc
      <> Opt.progDesc "Generate test data into DESTINATION"
      <> Opt.header "filesort-gen - data seeder" )

app :: Config -> IO ()
app (Config fp limit)
  = IO.withFile fp IO.WriteMode go
  where
    go h
      = S.runStream
      . S.serially
      . S.toHandle h
      . S.map show
      $ S.fromList [limit,(limit-1)..0]

config :: Opt.Parser Config
config
  = Config
  <$> Opt.strOption
      ( Opt.long "out"
      <> Opt.metavar "DESTINATION"
      <> Opt.help "Where to generate data" )
  <*> Opt.option Opt.auto
      ( Opt.long "limit"
      <> Opt.help "Number of rows to generate"
      <> Opt.showDefault
      <> Opt.value 10000 )

data Config = Config FilePath Natural
  deriving Show
