use actix_web::{web, App, HttpServer};
use std::sync::Arc;

// Importing modules containing functionalities
mod fetch_discriminators;
mod graph_disc;
mod query;
mod solana_connection;

// Importing specific functionalities from the modules
use fetch_discriminators::DiscriminatorFetcher;
use graph_disc::GraphDatabase;
use query::{query_discriminators_endpoint, query_instructions_endpoint,  upload_discriminator_endpoint };
use solana_connection::SolanaConnection;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello World!");
    let db = Arc::new(GraphDatabase::new("http://localhost:8529", "root", "Jayyu@1234", "disc_dir").await.unwrap());

    let solana_client = Arc::new(SolanaConnection::new("https://api.devnet.solana.com"));

    let program_id = "2heNN2tyetUwzRFjnLixpJYZC3TYF7rEEcxUivKoMAZg"; // replace with an actual program ID


    match db.query_discriminators(program_id).await{
        Ok(instructions) => println!("{:?}", instructions),
        Err(e) => println!("error")
    }

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(db.clone()))
            .app_data(web::Data::from(solana_client.clone()))
            .service(
                web::scope("")
                    .route("/upload_discriminator/{program_id}", web::post().to(upload_discriminator_endpoint))
                    .route("/query_discriminators/{program_id}", web::get().to(query_discriminators_endpoint))
                    .route("/query_instructions/{discriminator_id}", web::get().to(query_instructions_endpoint))
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
