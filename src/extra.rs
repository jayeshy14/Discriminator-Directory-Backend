use actix_web::{web, App, HttpServer};
use tokio::task;
use std::sync::Arc;
use actix_cors::Cors;

// Importing modules containing functionalities
mod graph_disc;
mod query;
mod solana_connection;

// Importing specific functionalities from the modules
use graph_disc::GraphDatabase;
use query::{query_discriminators_endpoint, query_instructions_endpoint,  upload_discriminator_endpoint };
use solana_connection::SolanaConnection;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello World!");
    let db = Arc::new(GraphDatabase::new("http://localhost:8529", "root", "Jayyu@1234", "disc_dir").await.unwrap());

        // Create the required collections if they don't exist
        db.create_required_collections().await.unwrap();

    let solana_client = Arc::new(SolanaConnection::new("https://api.devnet.solana.com"));

        // Fetch the list of program IDs from the database
        let program_ids = db.get_all_program_ids().await.unwrap();

    for program_id in program_ids {
        let db_clone = db.clone();
        let solana_client_clone = solana_client.clone();
        let program_id_clone = program_id.clone();

        // Spawn a new task for the real-time listener
        task::spawn(async move {
            (&solana_client_clone).real_time_listener(db_clone, program_id_clone).await;
        });
    }
    
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(db.clone()))
            .app_data(web::Data::from(solana_client.clone()))
            .wrap(Cors::default() // CORS configuration
            .allow_any_origin()  // Allow any origin for testing purposes, restrict in production
            .allow_any_method()
            .allow_any_header()
        )
            .service(
                web::scope("")
                    .route("/", web::get().to(|| async { "Hello World!" }))
                    .route("/upload_discriminator/{program_id}", web::post().to(upload_discriminator_endpoint))
                    .route("/query_discriminators/{program_id}", web::get().to(query_discriminators_endpoint))
                    .route("/query_instructions/{discriminator_id}", web::get().to(query_instructions_endpoint))
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}


use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use serde::Deserialize;
use solana_client::rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionEncoding};
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

    pub fn get_transactions(&self, program_id: &str) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, Box<dyn std::error::Error>>{
        let client = self.client.clone();
        let program_pubkey = match Pubkey::from_str(program_id) {
            Ok(pubkey) => pubkey,
            Err(e) => return Err(e.to_string().into()),
        };

        // Fetch signatures
        let signatures = client.get_signatures_for_address(&program_pubkey)?;
        Ok(signatures)
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



use arangors::client::reqwest::ReqwestClient;
use arangors::database::Database;
use arangors::document::options::InsertOptions;
use arangors::Connection;
use arangors::ClientError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::join;
use thiserror::Error;

// Structs for representing documents in the ArangoDB
#[derive(Debug, Serialize, Deserialize)]
pub struct Program {
    _key: String,
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Discriminator {
    _key: String,
    value: String,
    instruction: String,
    user_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Instruction {
    _key: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    _key: String,
    id: String,
}

// Structs for representing edges in the ArangoDB graph
#[derive(Debug, Serialize, Deserialize)]
pub struct HasDiscriminator {
    _from: String,
    _to: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MappedTo {
    _from: String,
    _to: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContributedBy {
    _from: String,
    _to: String,
}

// Custom error type to handle database-related errors
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("ArangoDB client error: {0}")]
    ClientError(#[from] ClientError),

    #[error("Failed to insert document into {collection}: {source}")]
    DocumentInsertionError {
        collection: String,
        #[source]
        source: ClientError,
    },

    #[error("Failed to execute AQL query: {query} - {source}")]
    AqlQueryError {
        query: String,
        #[source]
        source: ClientError,
    },
}

// Struct for interacting with the ArangoDB graph database
pub struct GraphDatabase {
    db: Arc<Database<ReqwestClient>>,
    program_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    discriminator_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    instruction_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    user_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    has_discriminator_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    mapped_to_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
    contributed_by_collection: Arc<arangors::collection::Collection<ReqwestClient>>,
}

// Implement the Clone trait for GraphDatabase to allow cloning
impl Clone for GraphDatabase {
    fn clone(&self) -> Self {
        GraphDatabase {
            db: Arc::clone(&self.db),
            program_collection: Arc::clone(&self.program_collection),
            discriminator_collection: Arc::clone(&self.discriminator_collection),
            instruction_collection: Arc::clone(&self.instruction_collection),
            user_collection: Arc::clone(&self.user_collection),
            has_discriminator_collection: Arc::clone(&self.has_discriminator_collection),
            mapped_to_collection: Arc::clone(&self.mapped_to_collection),
            contributed_by_collection: Arc::clone(&self.contributed_by_collection),
        }
    }
}

impl GraphDatabase {
    // Function to initialize a new GraphDatabase instance
    pub async fn new(uri: &str, user: &str, password: &str, db_name: &str) -> Result<Self, Box<dyn Error>> {
        let connection = Connection::establish_jwt(uri, user, password).await?;
        let db = connection.db(db_name).await?;

        let program_collection = db.collection("Programs").await?;
        let discriminator_collection = db.collection("Discriminators").await?;
        let instruction_collection = db.collection("Instructions").await?;
        let user_collection = db.collection("Users").await?;
        let has_discriminator_collection = db.collection("HasDiscriminator").await?;
        let mapped_to_collection = db.collection("MappedTo").await?;
        let contributed_by_collection = db.collection("ContributedBy").await?;

        Ok(GraphDatabase {
            db: Arc::new(db),
            program_collection: Arc::new(program_collection),
            discriminator_collection: Arc::new(discriminator_collection),
            instruction_collection: Arc::new(instruction_collection),
            user_collection: Arc::new(user_collection),
            has_discriminator_collection: Arc::new(has_discriminator_collection),
            mapped_to_collection: Arc::new(mapped_to_collection),
            contributed_by_collection: Arc::new(contributed_by_collection),
        })
    }

    // Function to upload a discriminator to the database
    pub async fn upload_discriminator(
        &self,
        program_id: &str,
        discriminator: &str,
        instruction: &str,
        user_id: &str,
    ) -> Result<(), DatabaseError> {



        // Create nodes for the collections
        let program = Program {
            _key: program_id.to_string(),
            id: program_id.to_string(),
        };
        let discriminator_doc = Discriminator {
            _key: sanitize_key(&format!("{}_{}", program_id, discriminator)),
            value: discriminator.to_string(),
            instruction: instruction.to_string(),
            user_id: user_id.to_string(),
        };
        let instruction_doc = Instruction {
            _key: sanitize_key(&format!("{}_{}", program_id, instruction)),
            value: instruction.to_string(),
        };
        let user = User {
            _key: user_id.to_string(),
            id: user_id.to_string(),
        };

        // Concurrently insert the documents into the collections
        let (res1, res2, res3, res4) = join!(
            self.program_collection.create_document(program, InsertOptions::builder().overwrite(true).build()),
            self.discriminator_collection.create_document(discriminator_doc, InsertOptions::builder().overwrite(true).build()),
            self.instruction_collection.create_document(instruction_doc, InsertOptions::builder().overwrite(true).build()),
            self.user_collection.create_document(user, InsertOptions::builder().overwrite(true).build())
        );

        

        // Check for errors
        res1.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "Programs".to_string(),
            source: e,
        })?;
        res2.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "Discriminators".to_string(),
            source: e,
        })?;
        res3.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "Instructions".to_string(),
            source: e,
        })?;
        res4.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "Users".to_string(),
            source: e,
        })?;

        // Create edges for the graph
        let edge_has_discriminator = HasDiscriminator {
            _from: format!("Programs/{}", program_id),
            _to: format!("Discriminators/{}_{}", program_id, discriminator),
        };
        let edge_mapped_to = MappedTo {
            _from: format!("Discriminators/{}_{}", program_id, discriminator),
            _to: format!("Instructions/{}_{}", program_id, instruction),
        };
        let edge_contributed_by = ContributedBy {
            _from: format!("Discriminators/{}_{}", program_id, discriminator),
            _to: format!("Users/{}", user_id),
        };

        // Insert edges concurrently
        let (edge_res1, edge_res2, edge_res3) = join!(
            self.has_discriminator_collection.create_document(edge_has_discriminator, InsertOptions::builder().overwrite(true).build()),
            self.mapped_to_collection.create_document(edge_mapped_to, InsertOptions::builder().overwrite(true).build()),
            self.contributed_by_collection.create_document(edge_contributed_by, InsertOptions::builder().overwrite(true).build())
        );

        // Check for errors
        edge_res1.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "HasDiscriminator".to_string(),
            source: e,
        })?;
        edge_res2.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "MappedTo".to_string(),
            source: e,
        })?;
        edge_res3.map_err(|e| DatabaseError::DocumentInsertionError {
            collection: "ContributedBy".to_string(),
            source: e,
        })?;

        Ok(())
    }

    // Function to query discriminators and their associated instructions from the database
    pub async fn query_discriminators_and_instructions(&self, program_id: &str) -> Result<Vec<Discriminator>, DatabaseError> {
        let aql = "
        FOR d IN Discriminators
            FILTER d._key LIKE @program_id
            LET instructions = (
                FOR i IN Instructions
                    FILTER i._key LIKE CONCAT(d._key, '%')
                    RETURN i
            )
            RETURN { discriminator: d, instructions: instructions }
        ";
        let mut bind_vars = HashMap::new();
        bind_vars.insert("program_id", format!("{}%", program_id).into());

        let results: Vec<HashMap<String, serde_json::Value>> = self.db.aql_bind_vars(aql, bind_vars).await
            .map_err(|e| DatabaseError::AqlQueryError { query: aql.to_string(), source: e })?;

        let mut discriminators = Vec::new();
        for result in results {
            let discriminator: Discriminator = serde_json::from_value(result.get("discriminator").unwrap().clone()).unwrap();
            let instructions: Vec<Instruction> = serde_json::from_value(result.get("instructions").unwrap().clone()).unwrap();
            let instruction_map = serde_json::to_string(&instructions).unwrap();
            discriminators.push(Discriminator {
                _key: discriminator._key,
                value: discriminator.value,
                instruction: instruction_map,
                user_id: discriminator.user_id,
            });
        }

        Ok(discriminators)
    }

    // Function to fetch instructions based on a single discriminator
    pub async fn fetch_instructions_by_discriminator(&self, discriminator_id: &str) -> Result<Vec<Instruction>, DatabaseError> {
        let aql = "
        FOR i IN Instructions
            FILTER i._key LIKE @discriminator_id
            RETURN i
        ";
        
        let mut bind_vars = HashMap::new();
        // Use `Value::String` to create a serde_json::Value
        bind_vars.insert("discriminator_id", Value::String(format!("{}%", discriminator_id)));

        let instructions: Vec<Instruction> = self.db.aql_bind_vars(aql, bind_vars).await
            .map_err(|e| DatabaseError::AqlQueryError { query: aql.to_string(), source: e })?;

        Ok(instructions)
    }
    // Function to get all program IDs from the database
    pub async fn get_all_program_ids(&self) -> Result<Vec<String>, DatabaseError> {
        let aql = "FOR p IN Programs RETURN p.id";
        let program_ids: Vec<String> = self.db.aql_str(aql).await
            .map_err(|e| DatabaseError::AqlQueryError { query: aql.to_string(), source: e })?;
        Ok(program_ids)
    }

    // Function to create the required collections if they do not exist
    pub async fn create_required_collections(&self) -> Result<(), ClientError> {
        let collections = vec![
            "Programs",
            "Discriminators",
            "Instructions",
            "Users",
            "HasDiscriminator",
            "MappedTo",
            "ContributedBy"
        ];

        for collection_name in collections {
            let collection = self.db.collection(collection_name).await;
            if collection.is_err() {
                self.db.create_collection(collection_name).await?;
            }
        }
        Ok(())
    }



}

fn sanitize_key(input: &str) -> String {
    input.replace("/", "_") // Replace invalid characters with underscores or any valid character
}



use actix_web::{web, HttpRequest, HttpResponse, Responder};
use crate::graph_disc::GraphDatabase;
use crate::solana_connection::SolanaConnection;


pub async fn query_discriminators_endpoint(
    db: web::Data<GraphDatabase>,
    solana_client: web::Data<SolanaConnection>,
    program_id: web::Path<String>,
) -> impl Responder {
    let program_id = program_id.into_inner();

    // Check if discriminators are in the database
    let discriminators = db.query_discriminators(&program_id).await;

    match discriminators {
        Ok(discriminators) => {
            if !discriminators.is_empty() {
                HttpResponse::Ok().json(discriminators)
            } else {
                // If not found in DB, fetch from Solana
                let accounts_result = solana_client.get_program_accounts(&program_id);

                match accounts_result.await {
                    Ok(accounts) => {
                        let mut uploaded_any = false;
                        for (pub_key, account) in accounts {
                            // Extract the discriminator from account data
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
                                return HttpResponse::InternalServerError().body(e.to_string());
                            }

                            uploaded_any = true;
                        }

                        if uploaded_any {
                            let disc = db.query_discriminators(&program_id).await;
                            match disc {
                                Ok(disc) => HttpResponse::Ok().json(disc),
                                Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
                            }
                        } else {
                            HttpResponse::NotFound().body("No discriminators found in Solana accounts")
                        }
                    }
                    Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
                }
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}


pub async fn query_instructions_endpoint(
    db: web::Data<GraphDatabase>,
    discriminator_id: web::Path<String>,
) -> impl Responder {
    let discriminator_id = discriminator_id.into_inner();

    match db.query_instructions(&discriminator_id).await {
        Ok(instructions) => HttpResponse::Ok().json(instructions),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn upload_discriminator_endpoint(
    db: web::Data<GraphDatabase>,
    program_id: web::Path<String>,
    discriminator_info: web::Json<(String, String, String)>,
    req: HttpRequest,
) -> impl Responder {
    let program_id = program_id.into_inner();
    let (discriminator, instruction, _) = discriminator_info.into_inner();

    // Extract user_id from the headers
    let user_id = match req.headers().get("user_id") {
        Some(value) => match value.to_str() {
            Ok(v) => v.to_string(),
            Err(_) => return HttpResponse::BadRequest().body("Invalid user_id header value"),
        },
        None => return HttpResponse::BadRequest().body("Missing user_id header"),
    };

    match db
        .upload_discriminator(&program_id, &discriminator, &instruction, &user_id)
        .await
    {
        Ok(_) => HttpResponse::Ok().body("Discriminator uploaded successfully"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
