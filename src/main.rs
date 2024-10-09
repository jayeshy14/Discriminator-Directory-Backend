use actix_web::{web, App, HttpServer};
use std::sync::Arc;
use actix_cors::Cors;

// Importing modules containing functionalities
mod graph_disc;
mod query;
mod solana_connection;

// Importing specific functionalities from the modules
use graph_disc::GraphDatabase;
use query::{query_discriminators_endpoint,  upload_discriminator_endpoint };
use solana_connection::SolanaConnection;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Hello World!");

    // Try to create a database connection
    let db = match GraphDatabase::new("http://localhost:8529", "root", "Jayyu@1234", "disc_dir").await {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("Failed to connect to the database: {:?}", e);
            std::process::exit(1);
        }
    };

    println!("Successfully connected to the database.");


    let solana_client = Arc::new(SolanaConnection::new("https://api.devnet.solana.com"));

    // Fetch the list of program IDs from the database
    let program_ids = match db.get_all_program_ids().await {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("Failed to fetch program IDs: {:?}", e);
            std::process::exit(1);
        }
    };

    println!("Fetched {} program IDs", program_ids.len());

    // for program_id in program_ids {
    //     let db_clone = db.clone();
    //     let solana_client_clone = solana_client.clone();
    //     let program_id_clone = program_id.clone();
    

    //     tokio::spawn(async move {
    //         if let Err(e) = solana_client_clone.real_time_listener(db_clone, program_id_clone).await {
    //             eprintln!("Error in real time listener: {}", e);
    //         }
    //     });
        

    // }
    
    
    
    println!("Starting HTTP server on 127.0.0.1:8080");

    HttpServer::new( move || {
        App::new()
            .app_data(web::Data::from(db.clone()))
            .app_data(web::Data::from(solana_client.clone()))
            .wrap(Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
            )
            .service(
                web::scope("")
                    .route("/", web::get().to(|| async { "Hello World!" }))
                    .route("/upload_discriminator/{program_id}", web::post().to(upload_discriminator_endpoint))
                    .route("/query_discriminators/{program_id}", web::get().to(query_discriminators_endpoint))
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}