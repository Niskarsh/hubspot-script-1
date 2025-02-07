const hubspot = require('@hubspot/api-client');
require('dotenv').config();

class HubspotContactDealAssociation {
    constructor() {
        if (!process.env.HUBSPOT_ACCESS_TOKEN) {
            throw new Error('HUBSPOT_ACCESS_TOKEN is not set in environment variables');
        }
        this.hubspotClient = new hubspot.Client({ accessToken: process.env.HUBSPOT_ACCESS_TOKEN });
    }

    async getRecentContacts(offset = 0) {
        const timestamp = Date.now() - 24 * 60 * 60 * 1000; // 24 hours ago

        try {
            const searchCriteria = {
                filterGroups: [{
                    filters: [{
                        propertyName: 'createdate',
                        operator: 'GTE',
                        value: timestamp.toString(),
                    }],
                }],
                properties: ['lemlistjobpostingurl', 'company', 'createdate'],
                limit: 100,
                after: offset,
            };

            const searchResponse = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchCriteria);
            console.log(`Found ${searchResponse.total} contacts created in the last 24 hours`);
            console.log(`Processing batch of ${searchResponse.results.length} contacts starting from offset ${offset}`);

            // Log unique lemlistjobpostingurls in this batch
            const uniqueUrls = new Set(searchResponse.results
                .filter(contact => contact.properties.lemlistjobpostingurl)
                .map(contact => contact.properties.lemlistjobpostingurl));
            console.log(`Found ${uniqueUrls.size} unique job posting URLs in this batch`);

            return { results: searchResponse.results, total: searchResponse.total };
        } catch (error) {
            throw new Error(`Error fetching recent contacts: ${error.message}`);
        }
    }

    async searchDeal(lemlistJobPostingUrl) {
        try {
            const exactResponse = await this.hubspotClient.crm.deals.searchApi.doSearch({
                filterGroups: [{
                    filters: [{
                        propertyName: 'dealname',
                        operator: 'EQ',
                        value: lemlistJobPostingUrl.trim()
                    }]
                }],
                properties: ['dealname', 'createdate', 'dealstage'],
                limit: 100
            });

            if (exactResponse.total > 0) {
                console.log(`Found ${exactResponse.total} exact matches for deal: ${lemlistJobPostingUrl}`);
                return exactResponse.results;
            }

            // If no exact match found, try contains token search
            const containsResponse = await this.hubspotClient.crm.deals.searchApi.doSearch({
                filterGroups: [{
                    filters: [{
                        propertyName: 'dealname',
                        operator: 'CONTAINS_TOKEN',
                        value: lemlistJobPostingUrl.trim()
                    }]
                }],
                properties: ['dealname', 'createdate', 'dealstage'],
                limit: 100
            });

            // Filter for exact matches (case-insensitive)
            const matches = containsResponse.results.filter(deal => 
                deal.properties.dealname.toLowerCase().trim() === lemlistJobPostingUrl.toLowerCase().trim()
            );

            if (matches.length > 0) {
                console.log(`Found ${matches.length} case-insensitive matches for deal: ${lemlistJobPostingUrl}`);
            }

            return matches;
        } catch (error) {
            console.error(`Error searching for deal ${lemlistJobPostingUrl}:`, error.message);
            return [];
        }
    }

    async createDeal(dealName) {
        try {
            // First check for existing deals
            const existingDeals = await this.searchDeal(dealName);

            if (existingDeals && existingDeals.length > 0) {
                const existingDeal = existingDeals[0];
                console.log(`Using existing deal: ${existingDeal.id} for ${dealName}`);
                return existingDeal.id;
            }

            // Double-check right before creation to prevent race conditions
            const doubleCheck = await this.searchDeal(dealName);
            if (doubleCheck && doubleCheck.length > 0) {
                const existingDeal = doubleCheck[0];
                console.log(`Found deal in double-check: ${existingDeal.id} for ${dealName}`);
                return existingDeal.id;
            }

            // Create new deal only if no existing deals found
            const dealData = {
                properties: {
                    dealname: dealName.trim(),
                    dealstage: '236104964', // Example deal stage, replace with your own
                },
            };

            const deal = await this.hubspotClient.crm.deals.basicApi.create(dealData);
            console.log(`Created new deal: ${deal.id} for ${dealName}`);
            return deal.id;
        } catch (error) {
            console.error(`Error creating deal ${dealName}:`, error.message);
            throw error;
        }
    }

    async searchCompany(companyName) {
        if (!companyName) {
            console.warn('Company name is missing, skipping search.');
            return [];
        }

        try {
            const response = await this.hubspotClient.crm.companies.searchApi.doSearch({
                filterGroups: [{
                    filters: [{
                        propertyName: 'name',
                        operator: 'EQ',
                        value: companyName.trim(),
                    }],
                }],
                limit: 1
            });
            return response.results || [];
        } catch (error) {
            console.error(`Error searching for company ${companyName}:`, error.message);
            return [];
        }
    }

    async createCompany(companyName) {
        if (!companyName) {
            console.warn('Company name is missing, cannot create company.');
            return null;
        }

        try {
            const companyData = {
                properties: {
                    name: companyName.trim(),
                },
            };
            const company = await this.hubspotClient.crm.companies.basicApi.create(companyData);
            console.log(`Created new company: ${company.id}`);
            return company.id;
        } catch (error) {
            console.error(`Error creating company ${companyName}:`, error.message);
            throw error;
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
            const apiResponse = await this.hubspotClient.crm.associations.v4.batchApi.create(
                fromObjectType,
                toObjectType,
                BatchInputPublicAssociationMultiPost
            );
            console.log('Association created:', JSON.stringify(apiResponse, null, 2));
        } catch (e) {
            if (e.message === 'HTTP request failed' && e.response) {
                console.error('HTTP Response Error:', JSON.stringify(e.response.body, null, 2));
            } else {
                console.error('Error:', e);
            }
        }
    }

    async processContacts() {
        try {
            let { results: contacts, total } = await this.getRecentContacts();
            console.log(`Found ${contacts.length} contacts to process out of total ${total}`);
            let offset = 0;
            let count = 0;
            let uniqueUrls = new Set();
            let processedUrls = new Set();

            do {
                console.log(`Processing contacts from ${offset} to ${offset + contacts.length}`);
                for (const contact of contacts) {
                    console.log(`Processing contact ${count++}`);
                    const lemlistJobPostingUrl = contact.properties.lemlistjobpostingurl;
                    const companyName = contact.properties.company;

                    if (!lemlistJobPostingUrl) {
                        console.warn(`Skipping contact ${contact.id} due to missing lemlistjobpostingurl.`);
                        continue;
                    }

                    // Track unique URLs
                    uniqueUrls.add(lemlistJobPostingUrl);

                    console.log(`Processing contact ${contact.id} with lemlistJobPostingUrl: ${lemlistJobPostingUrl}`);

                    // Check or create deal
                    let dealId;
                    const deals = await this.searchDeal(lemlistJobPostingUrl);
                    if (deals.length > 0) {
                        dealId = deals[0].id;
                        console.log(`Found existing deal: ${dealId}`);
                    } else {
                        dealId = await this.createDeal(lemlistJobPostingUrl);
                    }

                    // Associate contact with deal
                    await this.createAssociation('deals', 'contacts', dealId, contact.id);

                    // Check or create company
                    let companyId;
                    if (companyName) {
                        const companies = await this.searchCompany(companyName);
                        if (companies.length > 0) {
                            companyId = companies[0].id;
                            console.log(`Found existing company: ${companyId}`);
                        } else {
                            companyId = await this.createCompany(companyName);
                            console.log(`Created new company: ${companyId}`);
                        }

                        // Ensure only one company is associated with the deal
                        const existingCompanyAssociations = await this.hubspotClient.crm.deals.associationsApi.getAll(
                            dealId,
                            'companies'
                        );

                        const associatedCompanyIds = new Set(existingCompanyAssociations.results.map(assoc => assoc.id));

                        if (!associatedCompanyIds.has(companyId)) {
                            await this.createAssociation('deals', 'companies', dealId, companyId);
                            console.log(`Associated company ${companyId} with deal ${dealId}`);
                        } else {
                            console.log(`Company ${companyId} is already associated with deal ${dealId}`);
                        }
                    } else {
                        console.warn(`Skipping company association for contact ${contact.id} due to missing company name.`);
                    }

                    processedUrls.add(lemlistJobPostingUrl);
                    console.log(`Processed URLs: ${processedUrls.size} out of ${uniqueUrls.size} unique URLs`);
                }
                offset += contacts.length;
                ({ results: contacts } = await this.getRecentContacts(offset));
            } while (contacts.length > 0 && offset < total);

            console.log('Final Statistics:');
            console.log(`Total contacts processed: ${count}`);
            console.log(`Total unique URLs found: ${uniqueUrls.size}`);
            console.log(`Total URLs processed: ${processedUrls.size}`);
        } catch (error) {
            console.error('Error processing contacts:', error);
        }
    }
}

if (require.main === module) {
    console.log('Starting HubSpot contact-deal-company association process...');
    const hubspotManager = new HubspotContactDealAssociation();
    (async () => {
        try {
            await hubspotManager.processContacts();
        } catch (error) {
            console.error('Error in the main process:', error);
        }
    })();
}

module.exports = HubspotContactDealAssociation;
