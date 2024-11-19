require('dotenv').config();
const hubspot = require('@hubspot/api-client');

const hubspotClient = new hubspot.Client({
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN
});

async function getAllDeals() {
    const deals = [];
    let after = undefined;
    
    try {
        do {
            const apiResponse = await hubspotClient.crm.deals.basicApi.getPage(
                100,
                after,
                ['dealname', 'pipeline', 'dealstage', 'amount', 'closedate'],
                undefined,
                undefined
            );
            
            deals.push(...apiResponse.results);
            after = apiResponse.paging?.next?.after;
        } while (after);

        return deals;
    } catch (error) {
        console.error('Error fetching deals:', error.message);
        throw error;
    }
}

async function findDuplicates(deals) {
    // Group deals by name and pipeline
    const groupedDeals = deals.reduce((acc, deal) => {
        // Skip deals with no name
        if (!deal.properties?.dealname) {
            console.log('Found deal with no name:', deal.id);
            return acc;
        }
        
        const dealName = deal.properties.dealname.toLowerCase().trim();
        const pipeline = deal.properties.pipeline || 'default';
        const key = `${dealName}|${pipeline}`;
        
        if (!acc[key]) {
            acc[key] = [];
        }
        acc[key].push(deal);
        return acc;
    }, {});

    // Filter groups with more than one deal
    const duplicates = Object.values(groupedDeals).filter(group => group.length > 1);
    
    // Log duplicate information
    console.log(`Found ${duplicates.length} groups of duplicate deals`);
    duplicates.forEach(group => {
        const primaryDeal = group[0];
        console.log(`\nDuplicate group for deal "${primaryDeal.properties.dealname}":`);
        group.forEach(deal => {
            console.log(`- ID: ${deal.id}`);
            console.log(`  Amount: ${deal.properties.amount || 'Not set'}`);
            console.log(`  Stage: ${deal.properties.dealstage || 'Not set'}`);
            console.log(`  Created: ${deal.createdAt}`);
        });
    });

    return duplicates;
}

async function mergeDeals(duplicateGroups) {
    for (const group of duplicateGroups) {
        try {
            // Sort deals by:
            // 1. Amount (highest first)
            // 2. Most advanced stage
            // 3. Creation date (oldest first)
            const sortedDeals = group.sort((a, b) => {
                // Compare amounts
                const amountA = parseFloat(a.properties.amount) || 0;
                const amountB = parseFloat(b.properties.amount) || 0;
                if (amountA !== amountB) return amountB - amountA;
                
                // Compare stages (assuming higher stage number is more advanced)
                const stageA = a.properties.dealstage || '';
                const stageB = b.properties.dealstage || '';
                if (stageA !== stageB) return stageB.localeCompare(stageA);
                
                // Compare creation dates
                return new Date(a.createdAt) - new Date(b.createdAt);
            });
            
            const primaryDeal = sortedDeals[0];
            const duplicatesToMerge = sortedDeals.slice(1);

            console.log(`Merging duplicates for deal: ${primaryDeal.properties.dealname}`);
            console.log(`Using primary deal ID ${primaryDeal.id} (Amount: ${primaryDeal.properties.amount}, Stage: ${primaryDeal.properties.dealstage})`);
            
            for (const duplicate of duplicatesToMerge) {
                await hubspotClient.apiRequest({
                    method: 'POST',
                    path: `/crm/v3/objects/deals/merge`,
                    body: {
                        primaryObjectId: primaryDeal.id,
                        objectIdToMerge: duplicate.id
                    }
                });
                console.log(`Successfully merged deal ID ${duplicate.id} into ${primaryDeal.id}`);
            }
        } catch (error) {
            console.error(`Error merging deals in group ${group[0].properties.dealname}:`, error.message);
            if (error.response) {
                console.error('Error details:', error.response.body);
            }
        }
    }
}

async function main() {
    try {
        console.log('Fetching all deals...');
        const deals = await getAllDeals();
        console.log(`Found ${deals.length} total deals`);

        console.log('Finding duplicates...');
        const duplicateGroups = await findDuplicates(deals);
        console.log(`Found ${duplicateGroups.length} groups of duplicates`);

        if (duplicateGroups.length > 0) {
            console.log('Starting merge process...');
            await mergeDeals(duplicateGroups);
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