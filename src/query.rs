use actix_web::{web, HttpRequest, HttpResponse, Responder};
use crate::graph_disc::GraphDatabase;
use crate::solana_connection::SolanaConnection;
use crate::DiscriminatorFetcher;

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
                return HttpResponse::Ok().json(discriminators);
            } else {
                // If not found in DB, fetch from Solana
                let accounts_result =  solana_client.get_program_accounts(&program_id);

                match accounts_result {
                    Ok(accounts) => {
                        // Fetch discriminators from accounts
                        let discriminators = DiscriminatorFetcher::fetch_discriminators(&accounts);
                        match discriminators {
                            Ok(discriminators) => {
                                // Save to database
                                for discriminator in &discriminators {
                                    if let Err(e) = db.upload_discriminator(&program_id, discriminator, "instruction_placeholder", "user_placeholder").await {
                                        return HttpResponse::InternalServerError().body(e.to_string());
                                    }
                                }
                                return HttpResponse::Ok().json(discriminators);
                            }
                            Err(e) => return HttpResponse::InternalServerError().body(e),
                        }
                    }
                    Err(e) => return HttpResponse::InternalServerError().body(e),
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

// pub async fn listen_program_endpoint(
//     db: web::Data<GraphDatabase>,
//     solana_client: web::Data<SolanaConnection>,
//     program_id: web::Path<String>,
// ) -> impl Responder {
//     let program_id = program_id.into_inner();

//     let result = solana_client.program_listener(&program_id, db.get_ref().clone()).await;

//     match result {
//         Ok(_) => HttpResponse::Ok().body("Program listening started"),
//         Err(e) => HttpResponse::InternalServerError().body(e),
//     }
// }