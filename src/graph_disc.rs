use arangors::client::reqwest::ReqwestClient;
use arangors::database::Database;
use arangors::document::options::InsertOptions;
// use arangors::graph::{EdgeDefinition, Graph, GraphOptions};
use arangors::Connection;
use arangors::ClientError;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;



// Structs for representing documents in the ArangoDB
#[derive(Debug, Serialize, Deserialize)]
struct Program {
    _key: String,
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Discriminator {
    _key: String,
    value: String,
    instruction: String,
    user_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Instruction {
    _key: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    _key: String,
    id: String,
}

// Structs for representing edges in the ArangoDB graph
#[derive(Debug, Serialize, Deserialize)]
struct HasDiscriminator {
    _from: String,
    _to: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MappedTo {
    _from: String,
    _to: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContributedBy {
    _from: String,
    _to: String,
}

// Struct for interacting with the ArangoDB graph database
pub struct GraphDatabase {
    db: Arc<Database<ReqwestClient>>,
}

// Implement the Clone trait for GraphDatabase to allow cloning
impl Clone for GraphDatabase {
    fn clone(&self) -> Self {
        GraphDatabase {
            db: Arc::clone(&self.db),
        }
    }
}

impl GraphDatabase {
    // Function to initialize a new GraphDatabase instance
    pub async fn new(uri: &str, user: &str, password: &str, db_name: &str) -> Result<Self, Box<dyn Error>> {
        let connection = Connection::establish_jwt(uri, user, password).await?;
        let db = connection.db(db_name).await?;
        Ok(GraphDatabase {
            db: Arc::new(db),
        })
    }

    // Function to upload a discriminator to the database
    pub async fn upload_discriminator(
        &self,
        program_id: &str,
        discriminator: &str,
        instruction: &str,
        user_id: &str,
    ) -> Result<(), ClientError> {
        // Get references to the collections
        let program_collection = self.db.collection("Programs").await?;
        let discriminator_collection = self.db.collection("Discriminators").await?;
        let instruction_collection = self.db.collection("Instructions").await?;
        let user_collection = self.db.collection("Users").await?;
        let has_discriminator_collection = self.db.collection("HasDiscriminator").await?;
        let mapped_to_collection = self.db.collection("MappedTo").await?;
        let contributed_by_collection = self.db.collection("ContributedBy").await?;

        // Create nodes for the collections
        let program = Program {
            _key: program_id.to_string(),
            id: program_id.to_string(),
        };

        let discriminator_doc = Discriminator {
            _key: format!("{}_{}", program_id, discriminator),
            value: discriminator.to_string(),
            instruction: instruction.to_string(),
            user_id: user_id.to_string(),
        };

        let instruction_doc = Instruction {
            _key: format!("{}_{}", program_id, instruction),
            value: instruction.to_string(),
        };

        let user = User {
            _key: user_id.to_string(),
            id: user_id.to_string(),
        };

        // Insert nodes into the collections
        program_collection
            .create_document(program, InsertOptions::builder().overwrite(true).build())
            .await?;
        discriminator_collection
            .create_document(discriminator_doc, InsertOptions::builder().overwrite(true).build())
            .await?;
        instruction_collection
            .create_document(instruction_doc, InsertOptions::builder().overwrite(true).build())
            .await?;
        user_collection
            .create_document(user, InsertOptions::builder().overwrite(true).build())
            .await?;

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

        // Insert edges into the collections
        has_discriminator_collection
            .create_document(edge_has_discriminator, InsertOptions::builder().overwrite(true).build())
            .await?;
        mapped_to_collection
            .create_document(edge_mapped_to, InsertOptions::builder().overwrite(true).build())
            .await?;
        contributed_by_collection
            .create_document(edge_contributed_by, InsertOptions::builder().overwrite(true).build())
            .await?;

        Ok(())
    }

    // Function to query discriminators from the database
    pub async fn query_discriminators(&self, program_id: &str) -> Result<Vec<String>, ClientError> {
        let aql = format!(
            "FOR d IN Discriminators FILTER d._key LIKE '{}%' RETURN d.value",
            program_id
        );
        let discriminators: Vec<String> = self.db.aql_str(&aql).await?;
        Ok(discriminators)
    }

    // Function to query instructions from the database
    pub async fn query_instructions(&self, discriminator_id: &str) -> Result<Vec<String>, ClientError> {
        let aql = format!(
            "FOR i IN Instructions FILTER i._key LIKE '{}%' RETURN i.value",
            discriminator_id
        );
        let instructions: Vec<String> = self.db.aql_str(&aql).await?;
        Ok(instructions)
    }
}


