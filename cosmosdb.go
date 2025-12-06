package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/gofrs/uuid"
)

type PartitionKey struct {
	Key   string
	Value string
}

type CosmosDBOrderRepo struct {
	db           *azcosmos.ContainerClient
	partitionKey PartitionKey
}

func NewCosmosDBOrderRepoWithManagedIdentity(cosmosDbEndpoint string, dbName string, containerName string, partitionKey PartitionKey) (*CosmosDBOrderRepo, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("failed to create cosmosdb workload identity credential: %v\n", err)
		return nil, err
	}

	opts := azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
	}

	client, err := azcosmos.NewClient(cosmosDbEndpoint, cred, &opts)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	// create a cosmos container
	container, err := client.NewContainer(dbName, containerName)
	if err != nil {
		log.Printf("failed to create cosmosdb container: %v\n", err)
		return nil, err
	}

	return &CosmosDBOrderRepo{container, partitionKey}, nil
}

func NewCosmosDBOrderRepo(cosmosDbEndpoint string, dbName string, containerName string, cosmosDbKey string, partitionKey PartitionKey) (*CosmosDBOrderRepo, error) {
	cred, err := azcosmos.NewKeyCredential(cosmosDbKey)
	if err != nil {
		log.Printf("failed to create cosmosdb key credential: %v\n", err)
		return nil, err
	}

	// create a cosmos client
	client, err := azcosmos.NewClientWithKey(cosmosDbEndpoint, cred, nil)
	if err != nil {
		log.Printf("failed to create cosmosdb client: %v\n", err)
		return nil, err
	}

	// create a cosmos container
	container, err := client.NewContainer(dbName, containerName)
	if err != nil {
		log.Printf("failed to create cosmosdb container: %v\n", err)
		return nil, err
	}

	return &CosmosDBOrderRepo{container, partitionKey}, nil
}

func (r *CosmosDBOrderRepo) GetAllOrders() ([]Order, error) {
	var orders []Order

	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o", pk, nil)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to get next page: %v\n", err)
			return nil, err
		}

		for _, item := range queryResponse.Items {
			var order Order
			// Cosmos DB SQL API returns JSON, so it uses the `json:"..."` tags in your struct
			err := json.Unmarshal(item, &order)
			if err != nil {
				log.Printf("failed to deserialize order: %v\n", err)
				return nil, err
			}
			orders = append(orders, order)
		}
	}
	return orders, nil
}

// func (r *CosmosDBOrderRepo) GetPendingOrders() ([]Order, error) {
// 	var orders []Order

// 	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
// 	opt := &azcosmos.QueryOptions{
// 		QueryParameters: []azcosmos.QueryParameter{
// 			{Name: "@status", Value: Pending},
// 		},
// 	}
// 	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.status = @status", pk, opt)

// 	for queryPager.More() {
// 		queryResponse, err := queryPager.NextPage(context.Background())
// 		if err != nil {
// 			log.Printf("failed to get next page: %v\n", err)
// 			return nil, err
// 		}

// 		for _, item := range queryResponse.Items {
// 			var order Order
// 			err := json.Unmarshal(item, &order)
// 			if err != nil {
// 				log.Printf("failed to deserialize order: %v\n", err)
// 				return nil, err
// 			}
// 			orders = append(orders, order)
// 		}
// 	}
// 	return orders, nil
// }

func (r *CosmosDBOrderRepo) GetOrder(id string) (Order, error) {
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: id},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.orderId = @orderId", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to get next page: %v\n", err)
			return Order{}, err
		}

		for _, item := range queryResponse.Items {
			var order Order
			err := json.Unmarshal(item, &order)
			if err != nil {
				log.Printf("failed to deserialize order: %v\n", err)
				return Order{}, err
			}
			return order, nil
		}
	}
	return Order{}, nil
}

func (r *CosmosDBOrderRepo) InsertOrders(orders []Order) error {
	var counter = 0

	for _, o := range orders {
		pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

		marshalledOrder, err := json.Marshal(o)
		if err != nil {
			log.Printf("failed to marshal order: %v\n", err)
			return err
		}

		var order map[string]interface{}
		err = json.Unmarshal(marshalledOrder, &order)
		if err != nil {
			log.Printf("failed to unmarshal order: %v\n", err)
			return err
		}

		// add id with value of uuid.NewV4() to marhsalled order
		uuidWithHyphen, err := uuid.NewV4()
		if err != nil {
			log.Printf("failed to generate uuid: %v\n", err)
			return err
		}
		uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
		order["id"] = uuid

		order[r.partitionKey.Key] = r.partitionKey.Value

		marshalledOrder, err = json.Marshal(order)
		if err != nil {
			log.Printf("failed to marshal order: %v\n", err)
			return err
		}

		_, err = r.db.CreateItem(context.Background(), pk, marshalledOrder, nil)
		if err != nil {
			log.Printf("failed to create item: %v\n", err)
			return err
		}

		// increment counter for each order inserted
		counter++
	}

	log.Printf("Inserted %v documents into database\n", counter)

	return nil
}

func (r *CosmosDBOrderRepo) UpdateOrder(order Order) error {
	var existingOrderId string
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)

	// find the internal Cosmos DB 'id' using the application 'orderId'
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: order.OrderID},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.orderId = @orderId", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to query for update: %v\n", err)
			break
		}

		for _, item := range queryResponse.Items {
			var orderDoc map[string]interface{}
			err = json.Unmarshal(item, &orderDoc)
			if err != nil {
				log.Printf("failed to deserialize order doc: %v\n", err)
				return err
			}
			existingOrderId = orderDoc["id"].(string)
			break
		}
		if existingOrderId != "" {
			break
		}
	}

	if existingOrderId == "" {
		log.Printf("Order %s not found for update", order.OrderID)
		return nil
	}

	// create the patch operations
	patch := azcosmos.PatchOperations{}

	// update status
	patch.AppendReplace("/status", order.Status)

	// update items (critical for delete item functionality)
	patch.AppendReplace("/items", order.Items)

	// execute patch
	_, err := r.db.PatchItem(context.Background(), pk, existingOrderId, patch, nil)
	if err != nil {
		log.Printf("failed to patch item: %v\n", err)
		return err
	}

	return nil
}

// deletes an order by orderID
func (r *CosmosDBOrderRepo) DeleteOrder(id string) error {
	// find the internal Cosmos 'id' using the orderID
	var existingId string
	pk := azcosmos.NewPartitionKeyString(r.partitionKey.Value)
	opt := &azcosmos.QueryOptions{
		QueryParameters: []azcosmos.QueryParameter{
			{Name: "@orderId", Value: id},
		},
	}
	queryPager := r.db.NewQueryItemsPager("SELECT * FROM o WHERE o.orderId = @orderId", pk, opt)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			log.Printf("failed to query for delete: %v\n", err)
			return err
		}
		for _, item := range queryResponse.Items {
			var orderDoc map[string]interface{}
			if err := json.Unmarshal(item, &orderDoc); err == nil {
				existingId = orderDoc["id"].(string)
				break
			}
		}
		if existingId != "" {
			break
		}
	}

	if existingId == "" {
		log.Printf("No order found with ID %s to delete", id)
		return nil
	}

	// delete the item
	_, err := r.db.DeleteItem(context.Background(), pk, existingId, nil)
	if err != nil {
		log.Printf("failed to delete item: %v\n", err)
		return err
	}

	log.Printf("Deleted order with OrderID %s (Cosmos ID: %s)", id, existingId)
	return nil
}
