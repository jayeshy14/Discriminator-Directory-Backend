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

