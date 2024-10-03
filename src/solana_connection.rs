use std::str::FromStr;
use std::sync::Arc;
use solana_client::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use crate::graph_disc::GraphDatabase;
use solana_sdk::pubkey::Pubkey;


pub struct SolanaConnection {
    client: Arc<RpcClient>,
}

#[derive(Debug)]
struct ParsedEvent {
    program_id: String,
    discriminator: String,
    instruction: String,
    user_id: String,
}

impl SolanaConnection {
    pub fn new(url: &str) -> Self {
        let client = Arc::new(RpcClient::new_with_commitment(url.to_string(), CommitmentConfig::confirmed())) ;
        SolanaConnection { client }
    }

    pub fn get_program_accounts(&self, program_id: &str) -> Result<Vec<(Pubkey, Account)>, String> {
        let program_id = Pubkey::from_str(program_id).map_err(|e| e.to_string())?;
        self.client.get_program_accounts(&program_id).map_err(|e| e.to_string())
    }

    pub async fn real_time_listener(
        &self, 
        db: Arc<GraphDatabase>,
        program_id: String,
    ) {
        loop {
            // Fetch real-time events for the given program and account
            match self.get_program_accounts(&program_id) {
                Ok(accounts) => {
                    for (_, account) in accounts {
                        // Use a helper to parse event data
                        match self.parse_event( &account) {
                            Ok(parsed_event) => {
                                if let Err(e) = db.upload_discriminator(
                                    &parsed_event.program_id,
                                    &parsed_event.discriminator,
                                    &parsed_event.instruction,
                                    &parsed_event.user_id,
                                ).await {
                                    eprintln!("Failed to store event data: {}", e);
                                }
                            }
                            Err(e) => eprintln!("Failed to parse event: {}", e),
                        }
                    }
                }
                Err(e) => eprintln!("Error fetching real-time events: {:?}", e),
            }
    
            // Sleep for a short interval to prevent spamming the blockchain
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    }
    
    // Example parse_event function
    fn parse_event(&self, account: &Account) -> Result<ParsedEvent, String> {
        // Implement actual parsing logic based on your event structure
        if account.data.len() < 8 {
            return Err("Account data too short".to_string());
        }
    
        let discriminator = &account.data[0..8];
        let discriminator_str = hex::encode(discriminator);
    
        Ok(ParsedEvent {
            program_id: "program_id_placeholder".to_string(), // Extract from context or account
            discriminator: discriminator_str,
            instruction: "actual_instruction".to_string(), // Extract from account data
            user_id: "actual_user_id".to_string(), // Determine based on your logic
        })
    }
    


    // pub async fn program_listener(&self, program_id: &str, db: GraphDatabase) -> Result<(), String> {
    //     let program_key = Pubkey::from_str(program_id).map_err(|e| e.to_string())?;

    //     let accounts = self.client.get_program_accounts(&program_key).map_err(|e| e.to_string())?;

    //     for (_, account) in accounts {
    //         let discriminator = &account.data[0..8];
    //         let discriminator_str = hex::encode(discriminator);

    //         let result = db.upload_discriminator(program_id, &discriminator_str, "program_listener_placeholder", "user_placeholder").await;
    //         if result.is_err() {
    //             return Err(result.unwrap_err().to_string());
    //         }
    //     }

    //     Ok(())
    // }

}

// Implement the Clone trait manually
impl Clone for SolanaConnection {
    fn clone(&self) -> Self {
        SolanaConnection {
            client: Arc::clone(&self.client),
        }
    }
}