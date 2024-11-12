require('dotenv').config();
const hubspot = require('@hubspot/api-client');

const hubspotClient = new hubspot.Client({
    accessToken: process.env.HUBSPOT_ACCESS_TOKEN
});

async function getAllContacts() {
    const contacts = [];
    let after = undefined;
    
    try {
        do {
            const apiResponse = await hubspotClient.crm.contacts.basicApi.getPage(
                100,
                after,
                ['email', 'firstname', 'lastname', 'phone', 'mobilephone'],
                undefined,
                undefined
            );
            
            contacts.push(...apiResponse.results);
            after = apiResponse.paging?.next?.after;
        } while (after);

        return contacts;
    } catch (error) {
        console.error('Error fetching contacts:', error.message);
        throw error;
    }
}

async function findDuplicates(contacts) {
    // First group by email
    const groupedByEmail = contacts.reduce((acc, contact) => {
        if (contact.properties?.email) {
            const email = contact.properties.email.toLowerCase().trim();
            if (!acc[email]) {
                acc[email] = [];
            }
            acc[email].push(contact);
        }
        return acc;
    }, {});

    // Then group by full name (for contacts without email)
    const groupedByName = contacts.reduce((acc, contact) => {
        // Skip contacts that were already grouped by email
        if (contact.properties?.email) return acc;
        
        // Only group by name if both firstname and lastname exist
        if (contact.properties?.firstname && contact.properties?.lastname) {
            const fullName = `${contact.properties.firstname.toLowerCase().trim()} ${contact.properties.lastname.toLowerCase().trim()}`;
            if (!acc[fullName]) {
                acc[fullName] = [];
            }
            acc[fullName].push(contact);
        } else {
            console.log('Found contact with incomplete name data:', contact.id);
        }
        return acc;
    }, {});

    // Combine duplicates from both groups
    const emailDuplicates = Object.values(groupedByEmail).filter(group => group.length > 1);
    const nameDuplicates = Object.values(groupedByName).filter(group => group.length > 1);
    
    // Log duplicate information
    console.log(`Found ${emailDuplicates.length} groups of email duplicates`);
    console.log(`Found ${nameDuplicates.length} groups of name duplicates`);

    // Log email duplicates
    emailDuplicates.forEach(group => {
        const primaryContact = group[0];
        const contactName = `${primaryContact.properties.firstname || ''} ${primaryContact.properties.lastname || ''}`.trim();
        console.log(`\nDuplicate group for email "${primaryContact.properties.email}" (${contactName}):`);
        group.forEach(contact => {
            const name = `${contact.properties.firstname || ''} ${contact.properties.lastname || ''}`.trim();
            console.log(`- ID: ${contact.id}, Name: ${name}, Created: ${contact.createdAt}`);
        });
    });

    // Log name duplicates
    nameDuplicates.forEach(group => {
        const primaryContact = group[0];
        const contactName = `${primaryContact.properties.firstname} ${primaryContact.properties.lastname}`;
        console.log(`\nDuplicate group for name "${contactName}":`);
        group.forEach(contact => {
            console.log(`- ID: ${contact.id}, Created: ${contact.createdAt}`);
        });
    });

    // Return all duplicates
    return [...emailDuplicates, ...nameDuplicates];
}

async function mergeContacts(duplicateGroups) {
    for (const group of duplicateGroups) {
        try {
            // Sort by creation date and use the oldest as primary
            const sortedContacts = group.sort((a, b) => 
                new Date(a.createdAt) - new Date(b.createdAt)
            );
            
            const primaryContact = sortedContacts[0];
            const duplicatesToMerge = sortedContacts.slice(1);

            const contactName = `${primaryContact.properties.firstname || ''} ${primaryContact.properties.lastname || ''}`.trim();
            console.log(`Merging duplicates for contact: ${contactName} (${primaryContact.properties.email})`);
            
            for (const duplicate of duplicatesToMerge) {
                await hubspotClient.apiRequest({
                    method: 'POST',
                    path: `/crm/v3/objects/contacts/merge`,
                    body: {
                        primaryObjectId: primaryContact.id,
                        objectIdToMerge: duplicate.id
                    }
                });
                console.log(`Successfully merged contact ID ${duplicate.id} into ${primaryContact.id}`);
            }
        } catch (error) {
            console.error(`Error merging contacts in group ${group[0].properties.email}:`, error.message);
            if (error.response) {
                console.error('Error details:', error.response.body);
            }
        }
    }
}

async function main() {
    try {
        console.log('Fetching all contacts...');
        const contacts = await getAllContacts();
        console.log(`Found ${contacts.length} total contacts`);

        console.log('Finding duplicates...');
        const duplicateGroups = await findDuplicates(contacts);
        console.log(`Found ${duplicateGroups.length} groups of duplicates`);

        if (duplicateGroups.length > 0) {
            console.log('Starting merge process...');
            await mergeContacts(duplicateGroups);
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