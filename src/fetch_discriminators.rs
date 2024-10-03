use std::collections::HashSet;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use hex;

// Struct for fetching discriminators from Solana accounts
pub struct DiscriminatorFetcher;

impl DiscriminatorFetcher {
    // Function to fetch discriminators from a list of Solana accounts
    pub fn fetch_discriminators(accounts: &[(Pubkey, Account)]) -> Result<HashSet<String>, String> {
        let mut discriminators = HashSet::new();

        for (_, account) in accounts {
            // Extract the discriminator from the account data (first 8 bytes)
            let discriminator = &account.data[0..8];
            let discriminator_str = hex::encode(discriminator);
            // Insert the discriminator into the set
            discriminators.insert(discriminator_str);
        }
        Ok(discriminators)
    }
}
