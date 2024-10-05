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
    let discriminators = db.query_discriminators_and_instructions(&program_id).await;

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
                            let disc = db.query_discriminators_and_instructions(&program_id).await;
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

    match db.fetch_instructions_by_discriminator(&discriminator_id).await {
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
