use std::str::FromStr;
use std::sync::Arc;
use solana_client::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use crate::graph_disc::GraphDatabase;
use solana_sdk::pubkey::Pubkey;
use tokio::task; // Import tokio task for blocking operations

pub struct SolanaConnection {
    client: Arc<RpcClient>,
}

impl SolanaConnection {
    pub fn new(url: &str) -> Self {
        let client = Arc::new(RpcClient::new_with_commitment(url.to_string(), CommitmentConfig::confirmed()));
        SolanaConnection { client }
    }

    // Use spawn_blocking for RPC calls
    pub async fn get_program_accounts(&self, program_id: &str) -> Result<Vec<(Pubkey, Account)>, String> {
        let program_id = Pubkey::from_str(program_id).map_err(|e| e.to_string())?;
        let client = self.client.clone();

        // Move the RPC call to a blocking task
        task::spawn_blocking(move || {
            client.get_program_accounts(&program_id).map_err(|e| e.to_string())
        }).await.map_err(|e| e.to_string())?
    }

    pub async fn real_time_listener(
        &self, 
        db: Arc<GraphDatabase>,
        program_id: String,
    ) {
        loop {
            // Fetch real-time events for the given program and account using async RPC
            match self.get_program_accounts(&program_id).await {
                Ok(accounts) => {
                    for (pub_key, account) in accounts {
                        let discriminator = &account.data[0..8];
                        let discriminator_str = hex::encode(discriminator);
                        let instruction_data = &account.data[0..4];
                        let instruction = hex::encode(instruction_data); // Converts binary data to a hex string


                        if let Err(e) = db.upload_discriminator(
                            &program_id,
                            &discriminator_str,
                            &instruction,
                            &pub_key.to_string(),
                        ).await {
                            eprintln!("Failed to store event data: {}", e);
                        }
                    }
                }
                Err(e) => eprintln!("Error fetching real-time events: {:?}", e),
            }
    
            // Sleep for a short interval to prevent spamming the blockchain
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }
}

// Implement the Clone trait manually
impl Clone for SolanaConnection {
    fn clone(&self) -> Self {
        SolanaConnection {
            client: Arc::clone(&self.client),
        }
    }
}
