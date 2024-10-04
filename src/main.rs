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
