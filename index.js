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
        const timestamp = Date.now() - 72 * 60 * 60 * 1000; // 24 hours ago

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
                limit: 2,
                after: offset,
            };

            const searchResponse = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchCriteria);
            console.log(`Found ${searchResponse.total} contacts created in the last 24 hours`);
            return { results: searchResponse.results, total: searchResponse.total };
        } catch (error) {
            throw new Error(`Error fetching recent contacts: ${error.message}`);
        }
    }

    async searchDeal(lemlistJobPostingUrl) {
        if (!lemlistJobPostingUrl) {
            throw new Error('Deal name (lemlistJobPostingUrl) is required for search');
        }

        try {
            const response = await this.hubspotClient.crm.deals.searchApi.doSearch({
                filterGroups: [{
                    filters: [{
                        propertyName: 'dealname',
                        operator: 'EQ',
                        value: lemlistJobPostingUrl.trim(),
                    }],
                }],
            });
            return response.results || [];
        } catch (error) {
            throw new Error(`Error searching deal: ${error.message}`);
        }
    }

    async searchCompany(companyName) {
        if (!companyName) {
            throw new Error('Company name is required for search');
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
            });
            return response.results || [];
        } catch (error) {
            throw new Error(`Error searching company: ${error.message}`);
        }
    }

    async createDeal(dealName) {
        try {
            const dealData = {
                properties: {
                    dealname: dealName.trim(),
                    dealstage: '236104964', // Example deal stage, replace with your own
                },
            };
            const deal = await this.hubspotClient.crm.deals.basicApi.create(dealData);
            console.log(`Created new deal: ${deal.id}`);
            return deal.id;
        } catch (error) {
            throw new Error(`Error creating deal: ${error.message}`);
        }
    }

    async createCompany(companyName) {
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
            throw new Error(`Error creating company: ${error.message}`);
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
            console.log(`Found ${contacts.length} contacts to process`, total);
            let offset = 0;
            let count = 0;
            do {
                console.log(`Processing contacts from ${offset} to ${offset + contacts.length}`);
                  for (const contact of contacts) {
                // for (let i = 0; i < 1; i++) {
                    // const contact = contacts[i];
                    console.log(`Processing contact ${count++}`);
                    const lemlistJobPostingUrl = contact.properties.lemlistjobpostingurl;
                    const companyName = contact.properties.company;

                    if (!lemlistJobPostingUrl || !companyName) {
                        console.warn(`Skipping contact ${contact.id} due to missing properties.`);
                        continue;
                    }

                    console.log(`Processing contact ${contact.id} with lemlistJobPostingUrl: ${lemlistJobPostingUrl}`);
                    // console.log(contact);
                    // Check or create deal
                    let dealId;
                    const deals = await this.searchDeal(lemlistJobPostingUrl);
                    // console.log(deals);
                    if (deals.length > 0) {
                        dealId = deals[0].id;
                        console.log(`Found existing deal: ${dealId}`);
                    } else {
                        dealId = await this.createDeal(lemlistJobPostingUrl);
                    }
                    console.log(dealId);
                    // Associate contact with deal
                    let a = await this.createAssociation('deals', 'contacts', dealId, contact.id);
                    // // Check or create company
                    let companyId;
                    const companies = await this.searchCompany(companyName);
                    if (companies.length > 0) {
                        companyId = companies[0].id;
                        console.log(`Found existing company: ${companyId}`);
                    } else {
                        companyId = await this.createCompany(companyName);
                    }
                    // console.log(companies);

                    // Associate company with deal
                    await this.createAssociation('deals', 'companies', dealId, companyId);
                }
                offset += contacts.length;
                ({ results: contacts } = await this.getRecentContacts(offset));
            } while (offset < total);
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