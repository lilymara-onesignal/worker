use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::sync::Semaphore;

#[derive(thiserror::Error, Debug)]
#[error("worker has been shut down, this task will not be executed")]
pub struct Shutdown;

pub struct Worker {
    semaphore: Arc<Semaphore>,
    concurrency_limit: usize,
    is_shutdown: AtomicBool,
}

impl Worker {
    pub fn new(concurrency_limit: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(concurrency_limit)),
            concurrency_limit,
            is_shutdown: AtomicBool::new(false),
        }
    }

    pub fn execute(
        &self,
        future: impl 'static + Send + Sync + Future<Output = ()>,
    ) -> Result<(), Shutdown> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(Shutdown);
        }

        let semaphore = self.semaphore.clone();

        tokio::spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("Semaphore close api is not used");

            future.await;
            drop(permit);
        });

        Ok(())
    }

    pub async fn execute_wait(
        &self,
        future: impl 'static + Send + Sync + Future<Output = ()>,
    ) -> Result<(), Shutdown> {
        if self.is_shutdown.load(Ordering::Acquire) {
            return Err(Shutdown);
        }

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore close api is not used");

        tokio::spawn(async move {
            future.await;
            drop(permit);
        });

        Ok(())
    }

    pub async fn shutdown_join(&self) {
        self.is_shutdown.store(true, Ordering::SeqCst);

        _ = self
            .semaphore
            .acquire_many(self.concurrency_limit as u32)
            .await
            .expect("semaphore close api is not used");
    }
}
