// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace CmkScanner
{
    using System.Collections.Concurrent;
    using System.Diagnostics;
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
    public record ExpectedResult(bool DocumentFound);

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
        public static readonly string Query =
            "SELECT TOP 1 c.id != 0 as DocumentFound FROM c WHERE LENGTH(c.id) > 990";
        public static ConcurrentDictionary<string, int>? ContainerDocumentCount { get; set; }

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
        /// Get the amount of documents in the container. This query only runs after a 429 Too Many Requests
        /// exception happen. It will get the amount of documents in the container to split the query in batches.
        /// </summary>
        /// <param name="container">Affected container by the 429s.</param>
        /// <param name="cancellationToken">Cancellation token in case unexpected errors happen.</param>
        /// <returns>Amount of documents in the specific container.</returns>
        public static async Task<int> GetAmountOfDocumentsInContainerAsync(
            Container container,
            CancellationToken cancellationToken)
        {
            int amountOfDocuments = 0;
            using (FeedIterator<int> iterator = container.GetItemQueryIterator<int>(
                queryText: "SELECT VALUE COUNT(1) FROM c"))
            {
                while (iterator.HasMoreResults)
                {
                    FeedResponse<int> currentResultSet = await iterator.ReadNextAsync(cancellationToken);
                    amountOfDocuments += currentResultSet.FirstOrDefault();
                }
            }
            return amountOfDocuments;
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
            int retryAttemptsForTooManyRequests,
            int exponentialBaseValue,
            CancellationToken cancellationToken)
        {
            // Get the container to run the query.
            Container container = client.GetContainer(databaseName, containerName);

            // If account is affected for 429s, we will wait and retry with less data in batches.
            QueryRequestOptions queryRequestOptions = new()
            {
                MaxConcurrency = -1
            };

            if (retryAttemptsForTooManyRequests >= 1)
            {
                if (retryAttemptsForTooManyRequests == 1)
                {
                    // If it is the first attempt, we get the amount of documents in the container.
                    // to split the query in batches and optimize results.
                    int totalDocuments = await GetAmountOfDocumentsInContainerAsync(container, cancellationToken);
                    CmkScannerUtility.WriteScannerUpdate(
                        $"CMK Migration Scanner: Container {containerName} has {totalDocuments} documents. Distributing query in batches...");

                    bool amountOfDocsPerContainerAddedinDict = false;
                    int addDictAttempts = 0;
                    while (!amountOfDocsPerContainerAddedinDict && addDictAttempts < 3)
                    {
                        addDictAttempts++;
                        amountOfDocsPerContainerAddedinDict = ContainerDocumentCount!.TryAdd(containerName, totalDocuments);
                    }

                    if (!amountOfDocsPerContainerAddedinDict)
                    {
                        throw new Exception("Could not add the amount of documents in the container to the dictionary. Please run the program again.");
                    }
                }

                if (ContainerDocumentCount!.ContainsKey(containerName) == false)
                {
                    throw new Exception("ContainerDocumentCount does not contain the container. Please retry.");
                }

                // Calculate the batch of documents to get in the query. This value depends on the retry attempts and amount of documents.
                int documentsToGet =
                    (int)Math.Ceiling(ContainerDocumentCount![containerName] / Math.Pow(exponentialBaseValue, retryAttemptsForTooManyRequests));

                if (documentsToGet <= 1)
                {
                    throw new Exception("Retries exceeded for TooManyRequests exceptions.");
                }

                // We will only query this amount of data per batch.
                queryRequestOptions.MaxBufferedItemCount = documentsToGet;
            }

            // Get the iterator ready to run the query.
            FeedIterator<ExpectedResult> iterator = container.GetItemQueryIterator<ExpectedResult>(
                queryText: query,
                requestOptions: queryRequestOptions);

            // Analyze results.
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

                foreach (ExpectedResult res in currentResultSet)
                {
                    // Query optimized to stop when the first document is found.
                    if (res.DocumentFound)
                    {
                        CmkScannerUtility.WriteScannerUpdate(
                            $"In the container {container.Id}, there is at least one document with big id.",
                            ConsoleColor.Red);
                        return true;
                    }
                }
            }

            // DocumentFound is false or there were no results that met the
            // condition (currentResultSet.Count == 0).
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
        /// <returns>The Scanner Result. See ScannerResult enum for guidance</returns>
        /// <exception cref="TaskCanceledException"></exception>
        public static async Task<ScannerResult> ScanWithCosmosClientAsync(
            CosmosDBCredentialForScanner credentials,
            CosmosDBAuthType authType,
            int exponentialBaseValue)
        {
            CancellationTokenSource migrationFailedTokenSource = new();

            // Will store the amount of documents per container to optimize the query if fails.
            ContainerDocumentCount = new();

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
                bool keepTrying = true;
                bool? wereDocumentsWithBigIdsFound = null;
                int retryAttemptsForTooManyRequests = 0;

                // This loop works as a retry policy for scenarios when retriable exceptions happen.
                while (keepTrying)
                {
                    // First check if the token was cancelled. If so, throw an exception to stop the task.
                    if (migrationFailedTokenSource.IsCancellationRequested)
                    {
                        throw new TaskCanceledException();
                    }

                    CmkScannerUtility.WriteScannerUpdate(
                        $"Scanning container {dbAndCont.Container} in database {dbAndCont.Database}. Current time: {DateTime.Now}. Retry attempts: {retryAttemptsForTooManyRequests}...");
                    Stopwatch stopWatchPerRequest = new();

                    try
                    {
                        // Will look for documents with Big Ids. If found one, workflow ends with invalid.
                        wereDocumentsWithBigIdsFound = await SearchForDocumentsWithBigIdsAsync(
                            client,
                            Query,
                            dbAndCont.Database,
                            dbAndCont.Container,
                            retryAttemptsForTooManyRequests,
                            exponentialBaseValue,
                            migrationFailedTokenSource.Token);

                        // Query done. Exits loop.
                        keepTrying = false;
                    }
                    catch (Exception e)
                    {
                        if (e is not TaskCanceledException)
                        {
                            CmkScannerUtility.WriteScannerUpdate(
                                $"Exception: {e.Message}",
                                ConsoleColor.Red);

                            // 429 exception handling.
                            if (e is CosmosException cosmosException
                                && cosmosException.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                            {
                                retryAttemptsForTooManyRequests++;
                                TimeSpan? time = cosmosException.RetryAfter;
                                if (time != null)
                                {
                                    
                                    CmkScannerUtility.WriteScannerUpdate(
                                        $"Current time: {DateTime.Now}. Waiting for {time} before retrying.",
                                        ConsoleColor.Yellow);
                                    await Task.Delay(delay: (TimeSpan)time);
                                }
                            }
                            else
                            {
                                // Set flag to 1 to let other tasks know an error happened.
                                Interlocked.Exchange(ref unexpectedError, 1);
                                migrationFailedTokenSource.Cancel();
                                throw;
                            }
                        }
                        else
                        {
                            // If the task was cancelled, stop trying to search for documents.
                            keepTrying = false;
                        }
                    }
                }

                // Outside the retry loop, we check the result of the query.
                if (wereDocumentsWithBigIdsFound.HasValue && wereDocumentsWithBigIdsFound == true)
                {
                    // If a big id is found, set the atomic flag to 1 which means true.
                    Interlocked.Exchange(ref bigIdFound, 1);

                    // Cancel all other tasks, no need to keep searching.
                    migrationFailedTokenSource.Cancel();
                }
                else
                {
                    // If no big id was found, print a message to the user.
                    CmkScannerUtility.WriteScannerUpdate(
                        $"Scanning container {dbAndCont.Container} in database {dbAndCont.Database} finished. There were no big IDs found.",
                        ConsoleColor.Green);
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