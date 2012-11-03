{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | A Snaplet for using the Riak database (via the <http://hackage.haskell.org/package/riak riak> package)
-- Modelled on <http://hackage.haskell.org/package/snaplet-postgresql-simple snaplet-postgresql-simple>

module Snap.Snaplet.Riak
  ( RiakDB
  , withRiak
  , riakInit
  , riakCreate
  , HasRiak(getRiakState)
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

-- | Riak Snaplet state. Stores a connection pool shared between handlers.
data RiakDB = RiakDB
  { _pool :: Pool
  }

makeLens ''RiakDB

-- | A class which, when implemented, allows the wrapper functions below to
-- be used without explicitly managing the connection and having to use liftIO.

-- The wrappers are pretty mechanically defined, so Template Haskell could
-- probably be used instead- especially since there are so many options in
-- Network.Riak
class MonadIO m => HasRiak m where
  getRiakState :: m RiakDB

-- | Perform an action using a Riak Connection in the Riak snaplet
--
-- > result <- withRiak $ \c -> get c "myBucket" "myKey" Default
withRiak :: (HasRiak m) => (Connection -> IO a) -> m a
withRiak f = do
  c <- getRiakState
  liftIO $ withConnection (getL pool $ c) f

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

get ::
  (HasRiak m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> R -> m (Maybe (a, VClock))
get b k r = withRiak $ \conn -> R.get conn b k r

getMany ::
  (HasRiak m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> [Key] -> R -> m [Maybe (a, VClock)]
getMany b ks r = withRiak $ \conn -> R.getMany conn b ks r

modify ::
  (HasRiak m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO (a, b))
  -> m (a, b)
modify b k r w dw f = withRiak $ \conn -> R.modify conn b k r w dw f

modify_ ::
  (HasRiak m, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO a)
  -> m a
modify_ b k r w dw f = withRiak $ \conn -> R.modify_ conn b k r w dw f

delete :: (HasRiak m) => Bucket -> Key -> RW -> m ()
delete b k rw = withRiak $ \conn -> R.delete conn b k rw

put :: 
  (HasRiak m, Eq a, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> Key -> Maybe VClock -> a -> W -> DW -> m (a, VClock)
put b k vc c w dw = withRiak $ \conn -> R.put conn b k vc c w dw

putMany :: 
  (HasRiak m, Eq a, FromJSON a, ToJSON a, Resolvable a)
  => Bucket -> [(Key, Maybe VClock, a)] -> W -> DW -> m [(a, VClock)]
putMany b ks w dw = withRiak $ \conn -> R.putMany conn b ks w dw

listBuckets :: HasRiak m => m (Seq Bucket)
listBuckets = withRiak R.listBuckets

foldKeys :: HasRiak m => Bucket -> (a -> Key -> IO a) -> a -> m a
foldKeys b f x = withRiak $ \conn -> R.foldKeys conn b f x

getBucket :: HasRiak m => Bucket -> m BucketProps
getBucket b = withRiak $ \conn -> R.getBucket conn b

setBucket :: HasRiak m  => Bucket -> BucketProps -> m ()
setBucket b props = withRiak $ \conn -> R.setBucket conn b props

mapReduce :: HasRiak m => Job -> (a -> MapReduce -> a) -> a -> m a
mapReduce job f z0 = withRiak $ \conn -> R.mapReduce conn job f z0
