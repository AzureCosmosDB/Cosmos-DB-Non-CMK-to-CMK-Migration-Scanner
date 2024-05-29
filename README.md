# Customer Managed Keys (CMK) Migration Scanner

---
- [Customer Managed Keys (CMK) Migration Scanner](#customer-managed-keys-cmk-migration-scanner)
  - [Overview](#overview)
  - [Project Structure](#project-structure)
  - [Getting started](#getting-started)
    - [Clone the source code repository](#clone-the-source-code-repository)
    - [Run the program](#run-the-program)
      - [Using environment variables](#using-environment-variables)
        - [Usage example with environment variables](#usage-example-with-environment-variables)
      - [Using arguments](#using-arguments)
        - [Usage example with arguments](#usage-example-with-arguments)
  - [Help](#help)

## Overview

[Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/) allows a second layer of encryption with keys managed by customers. This feature is called [Customer-Managed Keys (CMK)](https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-customer-managed-keys?tabs=azure-portal).

In order to allow an existing Cosmos DB account to use to CMK, a scan needs to be done to ensure that the account doesn't have "big ids". A "big id" is a document id that exceeds 990 characters length. This scan is mandatory for the CMK migration and it is done by Microsoft automatically/ However, customers can replicate the scan result with this project. More info: https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-customer-managed-keys-existing-accounts

## Project Structure

The CMK Migration Tool project is a C# command-line executable. The tool consists on the main file, with an Utility files for validations, that scans by using one of the two available scanners (depending the APIs):

- For Mongo uses [REST](https://learn.microsoft.com/en-us/rest/api/cosmos-db/).
- For the Gremlin, Table and SQL uses [SDK: Cosmos Client](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-dotnet-v3).

## Getting started

### Clone the source code repository

From a command prompt, execute the following command in an empty working folder that will house the source code.

```shell
git clone https://github.com/AzureCosmosDB/Cosmos-DB-Non-CMK-to-CMK-Migration-Scanner.git
```

### Run the program

Move to the `CMK_Migration_Scanner` folder, the project files are located there. Before running the program, you need to select the authorization method to connect to your Azure Cosmos DB Account. You can choose up to three different authorization methods, which depends on the API type of your account. The options are shown as:

- If you are using **MongoDB**: Account Keys.
- If you are using **Table**: AAD or Account Keys.
- If you are using **SQL** or **Gremlin**: AAD, Account Keys or Connection String.

The next step is to provide your account details. Depending on the authorization method selected, the program could require some of this information:

- **Hostname:** Is the URI for the account.
- **Account Key:** Is the master key of your account. More info: [Primary/secondary keys.](https://learn.microsoft.com/en-us/azure/cosmos-db/secure-access-to-data?tabs=using-primary-key#primary-keys)
- **Connection String:** Is a string combination of the hostname and accounts keys. It is different depending on the API of the Cosmos DB account. More info: [Retrieve Connection Strings using CLI.](https://learn.microsoft.com/en-us/azure/cosmos-db/scripts/cli/common/keys)
- **Tenant ID:** Microsoft Entra ID entity in which you have your resources. More info: [Get Tenant ID](https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id).
- **Client ID:** Idenitifier of your App Registration. If you want to use AAD authorization, you will need to have a service prinicipal. Please follow the steps here, but only to [Create a Service Principal.](https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id)
- **Client Secret:** A client secret from the App Registration identified with your Client ID. More info: [How to Create a Secret](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal#option-3-create-a-new-client-secret).

The script allows to run using `environment variables` or `arguments`. Once you understood the information mentioned earlier, move to the the desired run method and check which are your required details.

There is an option to run the Scanner with [computed properties](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/computed-properties?tabs=dotnet#use-computed-properties-in-queries). **This is optional**. If this option is selected, your containers will have a new computed property and a new index only while the scan is running, which will run the query with less RUs.

#### Using environment variables

You will always need to set the variables:

CMK_AUTH_TYPE
CMK_API_TYPE

Then, set the following variables depending on your authorization method as it follows:

- If using **Account Keys**: CMK_HOSTNAME, CMK_ACCOUNT_KEY.
- If using **Connection String**: CMK_CONNECTION_STRING.
- If using **AAD**: CMK_TENANT_ID, CMK_CLIENT_ID, CMK_CLIENT_SECRET.

Finally, just run:

```shell
dotnet run Env
```

##### Usage example with environment variables

```shell
$env:CMK_CONNECTION_STRING = "AccountEndpoint=<uri>;AccountKey=<key>;"
$env:CMK_AUTH_TYPE = "ConnectionString"
$env:CMK_API_TYPE = "SQL"
dotnet run Env
```

#### Using arguments

You will always need to:

1) Choose auth method: AAD, AccountKey or ConnectionString.
2) Add `--api-type`: Could be MongoDB, SQL, Gremlin or Table.

- If using **Account Keys**: Add options: `--host-name` and `--account-key`.
- If using **Connection String**: Add option: `--connection-string`.
- If using **AAD**: Add options: `--host-name`, `--tenant-id`, `client-id` and `client-secret`.

Finally, just run:

```shell
dotnet run <auth-method> <options>
```

##### Usage example with arguments

```shell
dotnet run AAD --api-type Gremlin --host-name $hostname --tenant-id $tenantId --client-id $clientAppId --client-secret $clientSecretValue
```

## Help

For more help, you can use the command --h or -h. An example will be to run:

```shell
dotnet run -- -h
```
