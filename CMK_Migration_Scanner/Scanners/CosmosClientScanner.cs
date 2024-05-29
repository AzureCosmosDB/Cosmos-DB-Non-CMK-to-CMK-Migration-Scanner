// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace CmkScanner
{
    using System.Collections.Concurrent;
    using System.Collections.ObjectModel;

    using System.Security.Authentication;
    using Azure.Core;
    using Azure.Identity;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// This record has the expected format of the response from the Query.
    /// The Query searchs for documents with an ID bigger than 990, and if 
    /// it finds at least one, returns true and stops searching. Otherwise,
    ///  returns false or no response at all.
    /// </summary>
    public record ExpectedResult(string id);

    /// <summary>
    /// This class corresponds to the scanner that will be used to check if the
    /// account has IDs with a length greater than 990 for all the accounts
    /// of the following API types: SQL, Gremlin and Table.
    /// This scanner uses Microsoft.Azure.Cosmos SDK to connect to the account
    /// via Account Keys (with hostname and key or connection string) or AAD.
    /// </summary>
    public class CosmosClientScanner
    {
        // Query stops once it finds a document with a big ID.
        public const string kQueryWithComputedProperties =
            $"SELECT TOP 1 c.id FROM c WHERE c.{kComputedPropertyForDocumentLengthName} > 990";

        public const string kQuery =
            $"SELECT TOP 1 c.id FROM c WHERE LENGTH(c.id) > 990";

        public const string kComputedPropertyForDocumentLengthName = "TEMP_CosmosDBCmkMigration_DocumentIdLength";

        public const string kComputedPropertyForDocumentLengthQuery = "SELECT VALUE LENGTH(c.id) FROM c";

        public const string kIndexingPolicyPath = $"/{kComputedPropertyForDocumentLengthName}/?";

        /// <summary>
        /// Get all containers from the account using the CosmosClient.
        /// </summary>
        /// <param name="client">Current active cosmos client.</param>
        /// <returns>List of all containers of the Cosmos DB Account.</returns>
        /// <exception cref="Exception"></exception>
        public async static Task<List<DatabaseAndContainer>> GetAllContainersAsync(CosmosClient client)
        {
            // Setup async options. ConcurrentBag is thread safe.
            ConcurrentBag<DatabaseAndContainer> containers = [];
            CancellationTokenSource cancellationTokenSource = new();

            // Flag used as integer to be able to use Interlocked.Exchange between threads.
            int unexpectedError = 0;

            // Get all databases from the account.
            List<Database> databases = await GetDatabasesFromAccountAsync(client, cancellationTokenSource.Token);

            //Using Tasks as these are not heavy async operations, only get containers.
            List<Task> tasksToGetContainersPerDb = [];
            foreach (Database db in databases)
            {
                Task task = Task.Run(async () =>
                {
                    try
                    {
                        List<DatabaseAndContainer> containersFromDb =
                            await GetContainersFromDatabaseAsync(db, cancellationTokenSource.Token);

                        foreach (DatabaseAndContainer contAndDb in containersFromDb)
                        {
                            containers.Add(contAndDb);
                        }
                    }
                    catch (Exception e)
                    {
                        CmkScannerUtility.WriteScannerUpdate(
                            $"ERROR: Unexpected error when fetching containers from database {db.Id}. Exception: {e}",
                            ConsoleColor.Red);

                        // Set flag to 1 to let other tasks know an error happened.
                        Interlocked.Exchange(ref unexpectedError, 1);
                        cancellationTokenSource.Cancel();
                    }

                }, cancellationTokenSource.Token);

                // Add to task list to wait for all tasks to finish later on.
                tasksToGetContainersPerDb.Add(task);
            }

            // Wait for all tasks to finish.
            await Task.WhenAll(tasksToGetContainersPerDb);

            // If an unexpected error happened, throw exception. Else, return containers.
            return unexpectedError == 1
                ? throw new Exception(
                    "Unexpected error when fetching containers from account.")
                : containers.ToList();
        }

        /// <summary>
        /// Get all he databases from the account using the CosmosClient.
        /// </summary>
        /// <param name="client">Current active cosmos client.</param>
        /// <param name="cancellationToken">Cancellation token in case an unexpected 
        /// error happened or a big ID was already found.</param>
        /// <returns>List of all the databases in the Cosmos DB Account.</returns>
        public static async Task<List<Database>> GetDatabasesFromAccountAsync(
            CosmosClient client,
            CancellationToken cancellationToken)
        {
            List<Database> databases = [];
            using (FeedIterator<DatabaseProperties> iterator =
                client.GetDatabaseQueryIterator<DatabaseProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    foreach (DatabaseProperties db in await iterator.ReadNextAsync(cancellationToken))
                    {
                        databases.Add(client.GetDatabase(db.Id));
                    }
                }
            }
            return databases;
        }

        /// <summary>
        /// Get all containers from the database using the CosmosClient.
        /// </summary>
        /// <param name="database">Current database to extract the container available.</param>
        /// <param name="cancellationToken">Cancellation token in case an unexpected 
        /// error happened or a big ID was already found.</param>
        /// <returns>List of all containers from the specific database passed from parameter.</returns>
        public async static Task<List<DatabaseAndContainer>> GetContainersFromDatabaseAsync(
            Database database,
            CancellationToken cancellationToken)
        {
            List<DatabaseAndContainer> containers = [];
            using (FeedIterator<ContainerProperties> iterator =
                database.GetContainerQueryIterator<ContainerProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    foreach (ContainerProperties container in await iterator.ReadNextAsync(cancellationToken))
                    {
                        // Saves database and container names.
                        containers.Add(new DatabaseAndContainer(database.Id, container.Id));
                    }
                }
            }
            return containers;
        }

        /// <summary>
        /// Triggers the query to search for documents with big IDs in the container.
        /// Waits for the result and if a document with a big ID is found, it prints a message
        /// to let the customer know where it is located the document affected. It also throws 
        /// the cancellation token to stop the query as it is not needed to keep searching.
        /// </summary>
        /// <param name="client">Current active cosmos client.</param>
        /// <param name="query">Query to run in the container.</param>
        /// <param name="databaseName" >Database were the query is being excuted.</param>
        /// <param name="containerName">Container were the query is being excuted.</param>
        /// <param name="cancellationToken">Cancellation token in case an unexpected 
        /// error happened or a big ID was already found.</param>
        /// <returns>Boolean that indicates if a big id was found or not.</returns>
        public static async Task<bool> SearchForDocumentsWithBigIdsAsync(
            CosmosClient client,
            string query,
            string databaseName,
            string containerName,
            bool useComputedProperties,
            CancellationToken cancellationToken)
        {
            // Get the container to run the query.
            Container container = client.GetContainer(databaseName, containerName);

            // Read the current container properties.
            var containerProperties = await container.ReadContainerAsync(cancellationToken: cancellationToken);

            // Set computed properties if selected.
            if (useComputedProperties)
            {
                bool shouldUpdateContainer = false;
                // Make the necessary updates to the container properties.
                if (!containerProperties.Resource.ComputedProperties.Any(computedProperty => computedProperty.Name == kComputedPropertyForDocumentLengthName))
                {
                    containerProperties.Resource.ComputedProperties.Add(
                        new ComputedProperty
                        {
                            Name = kComputedPropertyForDocumentLengthName,
                            Query = kComputedPropertyForDocumentLengthQuery
                        });

                    shouldUpdateContainer = true;
                }

                if (!containerProperties.Resource.IndexingPolicy.IncludedPaths.Any(includePath => includePath.Path == kIndexingPolicyPath))
                {
                    containerProperties.Resource.IndexingPolicy.IncludedPaths.Add(
                        new IncludedPath()
                        {
                            Path = kIndexingPolicyPath                    
                        });

                    shouldUpdateContainer = true;
                }

                if (shouldUpdateContainer)
                {
                    // Update the container with the computed property to index the document IDs.
                    await container.ReplaceContainerAsync(containerProperties, cancellationToken: cancellationToken);
                }
            }

            // Get the iterator ready to run the query.
            FeedIterator<ExpectedResult> iterator =
                container.GetItemQueryIterator<ExpectedResult>(query);
            while (iterator.HasMoreResults)
            {
                // First validates that the cancellation token has not been cancelled.
                if (cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
                // Runs the query and gets the results.
                FeedResponse<ExpectedResult> currentResultSet =
                    await iterator.ReadNextAsync(cancellationToken);
                if (currentResultSet.Count > 0)
                {
                    CmkScannerUtility.WriteScannerUpdate(
                        $"In the container {container.Id}, there is at least one document with big id.",
                        ConsoleColor.Red);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Creates a Cosmos SDK Client with the given credentials and auth type.
        /// </summary>
        /// <param name="credentials">Account credentials</param>
        /// <param name="authType">Auth type to connect to Cosmos SDK</param>
        /// <returns>A Cosmos Client instance that will be used for all requests</returns>
        /// <exception cref="ArgumentException"></exception>
        public static CosmosClient GetCosmosClient(
            CosmosDBCredentialForScanner credentials,
            CosmosDBAuthType authType)
        {
            // Ensure you allow TLS 1.2 or update to your desired version.
            SslProtocols tlsVersion = SslProtocols.Tls12;

            // Configure the client options like TLS Version or certificates if needed.
            HttpMessageHandler clientCertificateHandler = new HttpClientHandler()
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                SslProtocols = tlsVersion,
                ServerCertificateCustomValidationCallback =
                    (httpRequestMessage, cert, cetChain, policyErrors) =>
                    {
                        return true;
                    }
            };

            CosmosClientOptions options = new()
            {
                // Connection mode set to Direct for better performance. Modify if needed.
                ConnectionMode = ConnectionMode.Direct,
                HttpClientFactory = () =>
                {
                    return new HttpClient(
                        clientCertificateHandler
                    );
                },
            };

            switch (authType)
            {
                case CosmosDBAuthType.ConnectionString:
                    return new(
                        connectionString: credentials.ConnectionString,
                        options);

                case CosmosDBAuthType.AccountKey:
                    return new(
                        accountEndpoint: credentials.Hostname,
                        authKeyOrResourceToken: credentials.AccountKey,
                        options);

                case CosmosDBAuthType.AAD:
                    TokenCredential servicePrincipal = new ClientSecretCredential(
                        credentials.TenantId,
                        credentials.ClientId,
                        credentials.ClientSecret);
                    return new(
                        accountEndpoint: credentials.Hostname,
                        tokenCredential: servicePrincipal,
                        options);

                default:
                    throw new ArgumentException("Invalid CosmosDBAuthType");
            }
        }

        /// <summary>
        /// Scanner initializer. It will connect to the Cosmos DB account and scan for documents.
        /// </summary>
        /// <param name="credentials">Account credentials</param>
        /// <param name="authType">Auth type to connect to Cosmos SDK</param>
        /// <param name="bool">Decides if should use or not computed properties.</param>
        /// <returns>The Scanner Result. See ScannerResult enum for guidance</returns>
        /// <exception cref="TaskCanceledException"></exception>
        public static async Task<ScannerResult> ScanWithCosmosClientAsync(
            CosmosDBCredentialForScanner credentials,
            CosmosDBAuthType authType,
            bool useComputedProperties)
        {
            CancellationTokenSource migrationFailedTokenSource = new();

            // Initialize the Cosmos client
            using CosmosClient client = GetCosmosClient(
                credentials,
                authType);

            // Cosmos DB Client connected!
            CmkScannerUtility.WriteScannerUpdate("Credentials validated.");

            // Get all containers and databases from the account.
            List<DatabaseAndContainer> containers;
            try
            {
                containers = await GetAllContainersAsync(client);
                CmkScannerUtility.WriteScannerUpdate($"Found {containers.Count} container(s) in the account.");
            }
            catch (Exception e)
            {
                CmkScannerUtility.WriteScannerUpdate(
                    $"ERROR: Unexpected error when fetching containers from account. Exception: {e}",
                    ConsoleColor.Red);

                return ScannerResult.UnexpectedErrorFound;
            }

            // Atomic flags used as integers to be able to use Interlocked.Exchange between threads.
            int unexpectedError = 0;
            int bigIdFound = 0;

            List<Task> tasks = containers.Select(dbAndCont => Task.Run(async () =>
            {
                try
                {
                    // First check if the token was cancelled. If so, throw an exception to stop the task.
                    if (migrationFailedTokenSource.IsCancellationRequested)
                    {
                        throw new TaskCanceledException();
                    }

                    // Will look for documents with Big Ids. If found one, workflow ends with invalid. 
                    bool wereDocumentsWithBigIdsFound = await SearchForDocumentsWithBigIdsAsync(
                        client,
                        useComputedProperties ? kQueryWithComputedProperties : kQuery,
                        dbAndCont.Database,
                        dbAndCont.Container,
                        useComputedProperties,
                        migrationFailedTokenSource.Token);

                    if (wereDocumentsWithBigIdsFound)
                    {
                        // If a big id is found, set the atomic flag to 1 which means true.
                        Interlocked.Exchange(ref bigIdFound, 1);

                        // Cancel all other tasks, no need to keep searching.
                        migrationFailedTokenSource.Cancel();
                    }
                }
                catch (Exception e)
                {
                    // Ignore if the error is a cancellation exception. Another thread already set the flag.
                    if (e is not TaskCanceledException && e is not OperationCanceledException)
                    {
                        // If an unexpected error is found, set this flag to 1 which means true.
                        Interlocked.Exchange(ref unexpectedError, 1);
                        CmkScannerUtility.WriteScannerUpdate(
                            $"ERROR: Unexpected error during CMK Migration Scan. Exception: {e}",
                            ConsoleColor.Red);

                        // Cancel all other tasks no matter the type of error.
                        migrationFailedTokenSource.Cancel();
                    }
                }
                finally
                {
                    if (useComputedProperties)
                    {
                        Container container = client.GetContainer(dbAndCont.Database, dbAndCont.Container);

                        // Read the current container properties.
                        var containerProperties = await container.ReadContainerAsync();
                        bool shouldUpdateContainer = false;
                        // Make the necessary updates to the container properties.
                        if (containerProperties.Resource.ComputedProperties.Any(computedProperty => computedProperty.Name == kComputedPropertyForDocumentLengthName))
                        {
                            ComputedProperty computedPropertyToRemove = containerProperties.Resource.ComputedProperties.First(computedProperty =>
                                computedProperty.Name == kComputedPropertyForDocumentLengthName);
                            containerProperties.Resource.ComputedProperties.Remove(computedPropertyToRemove);
                            shouldUpdateContainer = true;
                        }

                        if (containerProperties.Resource.IndexingPolicy.IncludedPaths.Any(includePath => includePath.Path == kIndexingPolicyPath))
                        {
                            IncludedPath includedPathToRemove = containerProperties.Resource.IndexingPolicy.IncludedPaths.First(includePath =>
                                includePath.Path == kIndexingPolicyPath);
                            containerProperties.Resource.IndexingPolicy.IncludedPaths.Remove(includedPathToRemove);
                            shouldUpdateContainer = true;
                        }

                        if (shouldUpdateContainer)
                        {
                            // Update the container with the computed property to index the document IDs.
                            await container.ReplaceContainerAsync(containerProperties);
                        }
                    }
                }

            }, migrationFailedTokenSource.Token)).ToList();

            // Wait for all tasks to finish.
            CmkScannerUtility.WriteScannerUpdate("Scanning...");
            await Task.WhenAll(tasks);

            return unexpectedError == 1
                ? ScannerResult.UnexpectedErrorFound
                : bigIdFound == 1
                    ? ScannerResult.CmkMigrationIsInvalid
                    : ScannerResult.CmkMigrationIsValid;
        }

    }
}