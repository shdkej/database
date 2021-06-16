package database

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Dynamodb struct {
	svc       *dynamodb.DynamoDB
	TableName string
}

func (conn *Dynamodb) Init() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	conn.svc = dynamodb.New(sess)
	err := conn.getTable()
	if err != nil {
		log.Println("failed to load table", err)
	}
	log.Println("DynamoDB Access")

	return nil
}

func (conn *Dynamodb) getTable() error {
	input := &dynamodb.ListTablesInput{}
	tableName := ""
	for {
		result, err := conn.svc.ListTables(input)
		if err != nil {
			log.Print(err)
			return err
		}
		for _, n := range result.TableNames {
			log.Println(*n)
			tableName = *n
			conn.TableName = tableName
			return nil
		}

		input.ExclusiveStartTableName = result.LastEvaluatedTableName
		if result.LastEvaluatedTableName == nil {
			break
		}
	}

	return nil
}

func (conn *Dynamodb) Hits(key string) (int64, error) {
	return 1, nil
}

func (conn *Dynamodb) Ping() error {
	return nil
}

func (conn *Dynamodb) Create(tag map[string]interface{}) error {
	av, err := dynamodbattribute.MarshalMap(tag)
	if err != nil {
		log.Print(err)
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(conn.TableName),
	}

	_, err = conn.svc.PutItem(input)
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func (conn *Dynamodb) Get(key string) (map[string]string, error) {
	result, err := conn.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(conn.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Tag": {
				S: aws.String(key),
			},
		},
	})
	if err != nil {
		log.Print(err)
		return nil, err
	}

	if result.Item == nil {
		return nil, nil
	}

	var item map[string]string
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		log.Printf("Unmarshal failed, item: %v, err: %v", result.Item, err)
		return nil, err
	}

	log.Println("Get: ", item)

	return item, nil
}

func (conn *Dynamodb) Update(key string, field string, value interface{}) error {
	_, err := conn.svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(conn.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Tag": {
				S: aws.String(key),
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (conn *Dynamodb) Delete(key string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Tag": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(conn.TableName),
	}
	_, err := conn.svc.DeleteItem(input)
	if err != nil {
		log.Print(err)
		return err
	}
	log.Println("Deleted : " + key)
	return nil
}

func (conn *Dynamodb) Scan(key string) ([]string, error) {
	filt := expression.Name("Tag").Equal(expression.Value(key))
	proj := expression.NamesList(expression.Name("Tag"))
	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		log.Println(err)
	}

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(conn.TableName),
	}

	result, err := conn.svc.Scan(params)
	if err != nil {
		log.Println(err)
	}

	log.Printf("item: %v", result.Items)

	var items []string
	for _, i := range result.Items {
		var item map[string]string
		err = dynamodbattribute.UnmarshalMap(i, &item)
		if err != nil {
			log.Print(err)
			return items, err
		}
		items = append(items, item["Name"])
	}

	return items, nil
}
