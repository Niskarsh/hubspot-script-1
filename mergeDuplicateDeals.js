require('dotenv').config();
const hubspot = require('@hubspot/api-client');

class HubspotDealMerger {
    constructor() {
        if (!process.env.HUBSPOT_ACCESS_TOKEN) {
            throw new Error('HUBSPOT_ACCESS_TOKEN is not set in environment variables');
        }
        this.hubspotClient = new hubspot.Client({ accessToken: process.env.HUBSPOT_ACCESS_TOKEN });
    }

    async getAllDeals() {
        const deals = [];
        let after = undefined;

        try {
            do {
                const apiResponse = await this.hubspotClient.crm.deals.basicApi.getPage(
                    100,
                    after,
                    ['dealname', 'createdate']
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

    async findDuplicates(deals) {
        const groupedByName = deals.reduce((acc, deal) => {
            const name = deal.properties.dealname.toLowerCase().trim();
            if (!acc[name]) {
                acc[name] = [];
            }
            acc[name].push(deal);
            return acc;
        }, {});

        const duplicates = Object.values(groupedByName).filter(group => group.length > 1);
        console.log(`Found ${duplicates.length} groups of duplicate deals`);
        return duplicates;
    }

    async transferAssociations(fromDealId, toDealId) {
        try {
            const associations = await this.hubspotClient.crm.associations.v4.batchApi.read(
                'deals',
                'contacts',
                { inputs: [{ id: fromDealId }] }
            );

            for (const association of associations.results) {
                await this.createAssociation('deals', 'contacts', toDealId, association.to.id);
            }

            const companyAssociations = await this.hubspotClient.crm.associations.v4.batchApi.read(
                'deals',
                'companies',
                { inputs: [{ id: fromDealId }] }
            );

            for (const association of companyAssociations.results) {
                await this.createAssociation('deals', 'companies', toDealId, association.to.id);
            }
        } catch (error) {
            console.error(`Error transferring associations from deal ${fromDealId} to ${toDealId}:`, error.message);
        }
    }

    async createAssociation(fromObjectType, toObjectType, fromObjectId, toObjectId) {
        let associationTypeId;
        if (fromObjectType === 'deals' && toObjectType === 'contacts') {
            associationTypeId = 3; // Deal to contact association type
        } else if (fromObjectType === 'deals' && toObjectType === 'companies') {
            associationTypeId = 341; // Deal to company association type
        } else {
            throw new Error(`Unsupported association between ${fromObjectType} and ${toObjectType}`);
        }

        const BatchInputPublicAssociationMultiPost = {
            inputs: [
                {
                    types: [{ associationCategory: 'HUBSPOT_DEFINED', associationTypeId }],
                    _from: { id: fromObjectId },
                    to: { id: toObjectId },
                },
            ],
        };

        try {
            await this.hubspotClient.crm.associations.v4.batchApi.create(
                fromObjectType,
                toObjectType,
                BatchInputPublicAssociationMultiPost
            );
        } catch (e) {
            console.error('Error creating association:', e.message);
        }
    }

    async mergeDeals(duplicateGroups) {
        for (const group of duplicateGroups) {
            try {
                const sortedDeals = group.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
                const primaryDeal = sortedDeals[0];
                const duplicatesToMerge = sortedDeals.slice(1);

                console.log(`Merging duplicates for deal: ${primaryDeal.properties.dealname}`);

                for (const duplicate of duplicatesToMerge) {
                    await this.transferAssociations(duplicate.id, primaryDeal.id);

                    await this.hubspotClient.crm.deals.basicApi.merge({
                        primaryObjectId: primaryDeal.id,
                        objectIdToMerge: duplicate.id
                    });

                    console.log(`Successfully merged deal ID ${duplicate.id} into ${primaryDeal.id}`);
                }
            } catch (error) {
                console.error(`Error merging deals in group ${group[0].properties.dealname}:`, error.message);
            }
        }
    }

    async mergeDuplicateDeals() {
        try {
            console.log('Fetching all deals...');
            const deals = await this.getAllDeals();
            console.log(`Found ${deals.length} total deals`);

            console.log('Finding duplicates...');
            const duplicateGroups = await this.findDuplicates(deals);
            console.log(`Found ${duplicateGroups.length} groups of duplicates`);

            if (duplicateGroups.length > 0) {
                console.log('Starting merge process...');
                await this.mergeDeals(duplicateGroups);
                console.log('Merge process completed');
            } else {
                console.log('No duplicates found');
            }
        } catch (error) {
            console.error('Error in main process:', error.message);
        }
    }
}

if (require.main === module) {
    console.log('Starting HubSpot deal merger process...');
    const hubspotDealMerger = new HubspotDealMerger();
    (async () => {
        try {
            await hubspotDealMerger.mergeDuplicateDeals();
        } catch (error) {
            console.error('Error in the main process:', error);
        }
    })();
}

module.exports = HubspotDealMerger; 