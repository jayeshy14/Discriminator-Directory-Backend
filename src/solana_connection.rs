use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use solana_client::{rpc_client::RpcClient, rpc_response::RpcConfirmedTransactionStatusWithSignature};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use solana_sdk::pubkey::Pubkey;
use tokio::task;

use crate::graph_disc::GraphDatabase; // Import tokio task for blocking operations

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
    
    

    pub async fn get_transactions(
        &self, 
        program_id: &str
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.clone();
        let program_pubkey = match Pubkey::from_str(program_id) {
            Ok(pubkey) => pubkey,
            Err(e) => return Err(e.to_string().into()),
        };
    
        // Use spawn_blocking to handle the synchronous part of this call.
        let signatures = task::spawn_blocking(move || {
            client.get_signatures_for_address(&program_pubkey)
        }).await??; // Double `?` to handle both Result from `spawn_blocking` and the actual function result.
    
        Ok(signatures)
    }
    

    // // Function to parse transactions and their instructions for a given program_id
    //  pub async fn parsed_instructions(&self, program_id: &str, db: Arc<GraphDatabase>,) -> Result<(), Box<dyn Error>> {

    //     let signatures = self.get_transactions(program_id)?;

    //     for signature in signatures {
    //         let tx_signature = Signature::from_str(&signature.signature)?;
    //         let transaction_result = self.client.get_transaction(&tx_signature, UiTransactionEncoding::Json)?;

    //         let transaction = transaction_result.transaction.transaction.decode().unwrap();
    //         let instructions = transaction.message.instructions();
    //         for instruction in instructions{
    //             let program_id = instruction.program_id_index.to_string();
    //             let accounts = instruction.accounts.clone();
    //             let data = instruction.data.clone();
    //             // // let discriminator = &data[0..8];
    //             // let discriminator_data: Value = serde_json::from_slice(&data[0..8])?;
    //             // let instruction_data: Value = serde_json::from_slice(&data[8..])?;

    //             let discriminator_data = data[0..8].to_vec();
    //             let instruction_data = data[8..].to_vec();

    //             // Extract user information from accounts or signers
    //             let contributing_account = accounts.get(0).unwrap(); 

    //             let user_id = contributing_account.to_string();
    //             db.upload_discriminator(
    //                 &program_id, 
    //                 discriminator_data, 
    //                 instruction_data, 
    //                 &user_id // Use user ID from accounts or signers
    //             ).await?;

    //         }

    //     }

    //     Ok(())
    // }
    

    pub async fn real_time_listener(
        &self, 
        db: Arc<GraphDatabase>,
        program_id: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            match self.get_transactions(&program_id).await {
                Ok(signatures) => {
                    for signature in signatures {
                        let tx_signature = match Signature::from_str(&signature.signature) {
                            Ok(sig) => sig,
                            Err(e) => {
                                eprintln!("Failed to parse signature: {}", e);
                                continue;
                            }
                        };

                        let transaction_result = match self.client.get_transaction(&tx_signature, UiTransactionEncoding::Json) {
                            Ok(result) => result,
                            Err(e) => {
                                eprintln!("Failed to get transaction: {}", e);
                                continue;
                            }
                        };

                        // Decode and process the transaction
                        if let Some(transaction) = transaction_result.transaction.transaction.decode() {
                            let instructions = transaction.message.instructions();

                            for instruction in instructions {
                                let program_id = instruction.program_id_index.to_string();
                                let accounts = instruction.accounts.clone();
                                let data = instruction.data.clone();
                                let discriminator_data = data[0..8].to_vec();
                                let instruction_data = data[8..].to_vec();
                                let user_id = accounts.get(0).unwrap().to_string();

                                // Store the extracted data in the database
                                if let Err(e) = db.upload_discriminator(
                                    &program_id,
                                    discriminator_data,
                                    instruction_data,
                                    &user_id,
                                ).await {
                                    eprintln!("Failed to store transaction data: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Error fetching transactions: {:?}", e),
            }

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
