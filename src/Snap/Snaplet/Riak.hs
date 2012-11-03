{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | A Snaplet for using the Riak database (via the 'Network.Riak' package)
module Snap.Snaplet.Riak
  ( RiakDB
  , withRiak
  , riakInit
  , riakCreate
  , HasRiak(riak)
  , get
  , getMany
  , modify
  , modify_
  , delete
  , put
  , putMany
  , listBuckets
  , foldKeys
  , getBucket
  , setBucket
  , mapReduce
  ) where

import Prelude hiding ((.))
import Control.Category ((.))
import Control.Monad.State (MonadState, gets)
import Control.Monad.IO.Class

import Data.Lens.Common
import Data.Lens.Template

import Snap.Snaplet

import Network.Riak (Connection, Client, Resolvable)
import Network.Riak.Types

import qualified Network.Riak as R
import Network.Riak.Protocol.BucketProps
import Network.Riak.Protocol.MapReduce
import Network.Riak.Connection.Pool

import Data.Time.Clock
import Data.Aeson.Types
import Data.Sequence

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

class HasRiak a where
  riak :: Lens a (Snaplet RiakDB)

-- | HasRiak wrappers around the database functions provided by Network.Riak.
-- The functions are pretty mechanically defined, so Template Haskell could
-- probably be used instead- especially since there are so many options...

get ::
  (HasRiak a, MonadIO m, MonadState a m, FromJSON c, ToJSON c, Resolvable c)
  => Bucket -> Key -> R -> m (Maybe (c, VClock))
get b k r = withRiak riak $ \conn -> R.get conn b k r

getMany ::
  (HasRiak a, MonadIO m, MonadState a m, FromJSON c, ToJSON c, Resolvable c)
  => Bucket -> [Key] -> R -> m [Maybe (c, VClock)]
getMany b ks r = withRiak riak $ \conn -> R.getMany conn b ks r

modify ::
  (HasRiak app, MonadIO m, MonadState app m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO (a, b))
  -> m (a, b)
modify b k r w dw f = withRiak riak $ \conn -> R.modify conn b k r w dw f

modify_ ::
  (HasRiak app, MonadIO m, MonadState app m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO a)
  -> m a
modify_ b k r w dw f = withRiak riak $ \conn -> R.modify_ conn b k r w dw f

delete :: (HasRiak a, MonadIO m, MonadState a m)
  => Bucket -> Key -> RW -> m ()
delete b k rw = withRiak riak $ \conn -> R.delete conn b k rw

put :: 
  (HasRiak a, MonadIO m, MonadState a m,
  Eq c, FromJSON c, ToJSON c, Resolvable c)
  => Bucket -> Key -> Maybe VClock -> c -> W -> DW -> m (c, VClock)
put b k vc c w dw = withRiak riak $ \conn -> R.put conn b k vc c w dw

putMany :: 
  (HasRiak a, MonadIO m, MonadState a m,
  Eq c, FromJSON c, ToJSON c, Resolvable c)
  => Bucket -> [(Key, Maybe VClock, c)] -> W -> DW -> m [(c, VClock)]
putMany b ks w dw = withRiak riak $ \conn -> R.putMany conn b ks w dw

listBuckets :: (HasRiak a, MonadIO m, MonadState a m) => m (Seq Bucket)
listBuckets = withRiak riak R.listBuckets

foldKeys :: (HasRiak a, MonadIO m, MonadState a m)
  => Bucket -> (a -> Key -> IO a) -> a -> m a
foldKeys b f x = withRiak riak $ \conn -> R.foldKeys conn b f x

getBucket :: (HasRiak a, MonadIO m, MonadState a m) => Bucket -> m BucketProps
getBucket b = withRiak riak $ \conn -> R.getBucket conn b

setBucket :: (HasRiak a, MonadIO m, MonadState a m)
  => Bucket -> BucketProps -> m ()
setBucket b props = withRiak riak $ \conn -> R.setBucket conn b props

mapReduce :: (HasRiak a, MonadIO m, MonadState a m) => Job -> (b -> MapReduce -> b) -> b -> m b
mapReduce job f z0 = withRiak riak $ \conn -> R.mapReduce conn job f z0
