{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | A Snaplet for using the Riak database (via the 'Network.Riak' package)
module Snap.Snaplet.Riak
  ( RiakDB
  , withRiak
  , riakInit
  , riakCreate
  ) where

import Prelude hiding ((.))
import Control.Category ((.))
import Control.Monad.State
import Control.Monad.IO.Class

import Data.Lens.Common
import Data.Lens.Template

import Snap.Snaplet

import Data.Time.Clock
import Network.Riak
import Network.Riak.Connection.Pool

-- | Riak Snaplet state
data RiakDB = RiakDB
  { _pool :: Pool
  }

makeLens ''RiakDB

-- | Perform an action using a Riak Connection in the Riak snaplet
--
-- > result <- withRiak $ \c -> get c "myBucket" "myKey" Default
withRiak :: (MonadIO m, MonadState app m) =>
  Lens app (Snaplet RiakDB) -> (Connection -> IO a) -> m a
withRiak snaplet f = do
  c <- gets $ getL (pool . snapletValue . snaplet)
  liftIO $ withConnection c f

-- | Utility function for creating the Riak snaplet from an Initializer
makeRiak :: Initializer b v v -> SnapletInit b v
makeRiak = makeSnaplet "snaplet-riak" "Riak Snaplet." Nothing

-- | Initialize the Riak snaplet
riakInit :: Pool -> SnapletInit b RiakDB
riakInit pool = makeRiak . return $ RiakDB pool

-- | Thin wrapper around 'Network.Riak.create' to run within 'SnapletInit'
riakCreate :: Client -> Int -> NominalDiffTime -> Int -> SnapletInit b RiakDB
riakCreate c ns t nc = makeRiak $ do
  pool <- liftIO $ create c ns t nc
  return $ RiakDB pool
