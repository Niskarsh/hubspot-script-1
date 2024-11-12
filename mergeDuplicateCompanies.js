require('dotenv').config();
const hubspot = require('@hubspot/api-client');

const hubspotClient = new hubspot.Client({
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN
});

async function getAllCompanies() {
    const companies = [];
    let after = undefined;
    
    try {
        do {
            const apiResponse = await hubspotClient.crm.companies.basicApi.getPage(
                100,
                after,
                ['name', 'domain'],
                undefined,
                undefined
            );
            
            companies.push(...apiResponse.results);
            after = apiResponse.paging?.next?.after;
        } while (after);

        return companies;
    } catch (error) {
        console.error('Error fetching companies:', error.message);
        throw error;
    }
}

async function findDuplicates(companies) {
    // Group companies by name
    const groupedByName = companies.reduce((acc, company) => {
        // Skip companies with no name
        if (!company.properties?.name) {
            console.log('Found company with no name:', company.id);
            return acc;
        }
        
        const name = company.properties.name.toLowerCase().trim();
        if (!acc[name]) {
            acc[name] = [];
        }
        acc[name].push(company);
        return acc;
    }, {});

    // Filter groups with more than one company
    const duplicates = Object.values(groupedByName).filter(group => group.length > 1);
    
    // Log some helpful information
    console.log(`Found ${duplicates.length} groups of duplicate companies`);
    duplicates.forEach(group => {
        console.log(`\nDuplicate group for "${group[0].properties.name}":`);
        group.forEach(company => {
            console.log(`- ID: ${company.id}, Created: ${company.createdAt}`);
        });
    });

    return duplicates;
}

async function mergeCompanies(duplicateGroups) {
    for (const group of duplicateGroups) {
        try {
            // Sort by creation date and use the oldest as primary
            const sortedCompanies = group.sort((a, b) => 
                new Date(a.createdAt) - new Date(b.createdAt)
            );
            
            const primaryCompany = sortedCompanies[0];
            const duplicatesToMerge = sortedCompanies.slice(1);

            console.log(`Merging duplicates for company: ${primaryCompany.properties.name}`);
            
            for (const duplicate of duplicatesToMerge) {
                await hubspotClient.apiRequest({
                    method: 'POST',
                    path: `/crm/v3/objects/companies/merge`,
                    body: {
                        primaryObjectId: primaryCompany.id,
                        objectIdToMerge: duplicate.id
                    }
                });
                console.log(`Successfully merged company ID ${duplicate.id} into ${primaryCompany.id}`);
            }
        } catch (error) {
            console.error(`Error merging companies in group ${group[0].properties.name}:`, error.message);
            // Log more details about the error
            if (error.response) {
                console.error('Error details:', error.response.body);
            }
        }
    }
}

async function main() {
    try {
        console.log('Fetching all companies...');
        const companies = await getAllCompanies();
        console.log(`Found ${companies.length} total companies`);

        console.log('Finding duplicates...');
        const duplicateGroups = await findDuplicates(companies);
        console.log(`Found ${duplicateGroups.length} groups of duplicates`);

        if (duplicateGroups.length > 0) {
            console.log('Starting merge process...');
            await mergeCompanies(duplicateGroups);
            console.log('Merge process completed');
        } else {
            console.log('No duplicates found');
        }
    } catch (error) {
        console.error('Error in main process:', error.message);
    }
}

// Run the script
main(); 