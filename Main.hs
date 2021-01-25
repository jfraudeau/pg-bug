{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Main where

import Control.Monad (void)
import Control.Monad.Logger (logDebugS, runStdoutLoggingT)
import Control.Monad.Reader (ReaderT)
import Data.Int (Int64)
import Data.Text (Text)
import Database.Esqueleto
import Database.Persist.Postgresql (withPostgresqlPool)
import Database.Persist.TH
  ( mkMigrate,
    mkPersist,
    persistLowerCase,
    share,
    sqlSettings,
  )
import UnliftIO (MonadIO, newEmptyMVar, waitBoth, withAsync)
import UnliftIO.Concurrent (threadDelay)

share
  [mkPersist sqlSettings, mkMigrate "migrateAll"]
  [persistLowerCase|
  Person
    name Text
    deriving Eq Show
|]

main :: IO ()
main = do
  runStdoutLoggingT $
    withPostgresqlPool "postgresql://postgres:pass@localhost" 10 $ \pool -> do
      runSqlPool (runMigration migrateAll) pool
      id <- runSqlPool insertInitialData pool
      let asyncJob threadName = do
            let transactionJob = do
                  $(logDebugS) threadName "Acquire Lock"
                  lockRow id
                  $(logDebugS) threadName "Run Updates"
                  updateRecord id
                  threadDelay 1000000
                  updateRecord id
                  updateRecord id
                  updateRecord id
                  updateRecord id
                  updateRecord id
                  updateRecord id

            $(logDebugS) threadName "Transaction Start"
            runSqlPool transactionJob pool
            $(logDebugS) threadName "Transaction Stop"
      withAsync (asyncJob "Thread-1") $ \t1 ->
        withAsync (asyncJob "Thread-2") $ \t2 ->
          void (waitBoth t1 t2)

lockRow ::
  ( MonadIO m,
    BackendCompatible SqlBackend backend,
    PersistQueryRead backend,
    PersistUniqueRead backend
  ) =>
  PersonId ->
  ReaderT backend m [Entity Person]
lockRow id = select $
  from $ \person -> do
    where_ $ person ^. PersonId ==. val id
    locking ForUpdate
    pure person

updateRecord ::
  ( MonadIO m,
    BackendCompatible SqlBackend backend,
    PersistQueryWrite backend,
    PersistUniqueWrite backend
  ) =>
  PersonId ->
  ReaderT backend m ()
updateRecord id = update $ \person -> do
  set person [PersonName =. val "Dummy"]
  where_ $ person ^. PersonId ==. val id

insertInitialData ::
  ( MonadIO m,
    PersistStoreWrite backend,
    PersistRecordBackend Person backend
  ) =>
  ReaderT backend m PersonId
insertInitialData = insert (Person "Dummy")