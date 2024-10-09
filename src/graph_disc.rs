use arangors::client::reqwest::ReqwestClient;
use arangors::database::Database;
use arangors::document::options::InsertOptions;
use arangors::AqlQuery;
use arangors::Connection;
use arangors::ClientError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::join;
use thiserror::Error;
use sha2::{Digest, Sha256};

// Structs for representing documents in the ArangoDB
#[derive(Debug, Serialize, Deserialize)]
pub struct Program {
    _key: String,
    id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Discriminator {
    _key: String,
    discriminator_id: String,
    discriminator_data: Vec<u8>,
    instruction: Instruction,
    user_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Instruction {
    _key: String,
    instruction_id: String,
    instruction_data: Vec<u8>,
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
            match db.collection(collection_name).await {
                Ok(_) => println!("Collection '{}' already exists.", collection_name),
                Err(_) => {
                    println!("Creating collection '{}'.", collection_name);
                    db.create_collection(collection_name).await?;
                    
                }
            }
        }

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

    // Function to hash keys
    fn hash_key(input: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(input);
        hex::encode(hasher.finalize())
    }

    // Function to upload a discriminator to the database
    pub async fn upload_discriminator(
        &self,
        program_id: &str,
        discriminator_data: Vec<u8>,
        instruction_data: Vec<u8>,
        user_id: &str,
    ) -> Result<(), DatabaseError> {

        let discriminator_id = hex::encode(discriminator_data.clone());
        let instruction_id = hex::encode(instruction_data.clone());

        let program = Program {
            _key: program_id.to_string(),
            id: program_id.to_string(),
        };

        let discriminator_key = format!("{}_{}", program_id, discriminator_id);
        let instruction_key = format!("{}_{}", program_id, Self::hash_key(&instruction_id));

        // Debug logs to print the keys
        println!("Program key: {}", program._key);
        println!("Discriminator key: {}", discriminator_key);
        println!("Instruction key: {}", instruction_key);

        let discriminator_doc = Discriminator {
            _key: discriminator_key.clone(),
            discriminator_id: discriminator_id.clone(),
            discriminator_data: discriminator_data.clone(),
            instruction: Instruction {
                _key: instruction_key.clone(),
                instruction_id: instruction_id.clone(),
                instruction_data: instruction_data.clone(),
            },
            user_id: user_id.to_string(),
        };

        let instruction_doc = Instruction {
            _key: instruction_key.clone(),
            instruction_id: instruction_id.clone(),
            instruction_data: instruction_data.clone(),
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
            _to: format!("Discriminators/{}", discriminator_key.clone()),
        };
        let edge_mapped_to = MappedTo {
            _from: format!("Discriminators/{}", discriminator_key),
            _to: format!("Instructions/{}", instruction_key.clone()),
        };
        let edge_contributed_by = ContributedBy {
            _from: format!("Discriminators/{}", discriminator_key),
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

    // Function to query discriminators and their instructions by program ID
    pub async fn query_discriminators_and_instructions(&self, program_id: &str) -> Result<Vec<Discriminator>, DatabaseError> {
        let aql = "
        FOR d IN Discriminators
            FILTER d._key LIKE @program_id
            RETURN {discriminator: d}
        ";

        let mut bind_vars = HashMap::new();
        bind_vars.insert("program_id", format!("{}%", program_id).into());

        let results: Vec<HashMap<String, Discriminator>> = self.db.aql_bind_vars(aql, bind_vars).await
            .map_err(|e| DatabaseError::AqlQueryError { query: aql.to_string(), source: e })?;

        let mut discriminators = Vec::new();
        for result in results {
            let discriminator: &Discriminator = match result.get("discriminator") {
                Some(discriminator) => &discriminator.clone(),
                None => {
                    panic!("Discriminator not found");
                }
            };

            discriminators.push(Discriminator {
                _key: discriminator._key.clone(),
                discriminator_id: discriminator.discriminator_id.clone(),
                instruction: discriminator.instruction.clone(),
                discriminator_data: discriminator.discriminator_data.clone(),
                user_id: discriminator.user_id.clone(),
            });
        }

        Ok(discriminators)
    }

    
    // Function to get all program IDs from the database
    pub async fn get_all_program_ids(&self) -> Result<Vec<String>, DatabaseError> {
        let aql = "FOR p IN Programs RETURN p.id";
        let program_ids: Vec<String> = self.db.aql_str(aql).await
            .map_err(|e| DatabaseError::AqlQueryError { query: aql.to_string(), source: e })?;
        Ok(program_ids)
    }
}
