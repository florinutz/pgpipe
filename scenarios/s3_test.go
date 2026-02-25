//go:build integration

package scenarios

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3adapter "github.com/florinutz/pgcdc/adapter/s3"
	"github.com/parquet-go/parquet-go"
	minitc "github.com/testcontainers/testcontainers-go/modules/minio"
)

// startMinIO starts a MinIO container and returns the endpoint URL.
func startMinIO(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	mc, err := minitc.Run(ctx, "minio/minio:latest")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })

	url, err := mc.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get minio endpoint: %v", err)
	}
	return "http://" + url
}

// newMinIOClient creates an S3 client pointed at the MinIO endpoint.
func newMinIOClient(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
}

// createBucket creates a bucket in MinIO.
func createBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
}

// listObjects lists all objects in a bucket with the given prefix.
func listObjects(t *testing.T, client *s3.Client, bucket, prefix string) []string {
	t.Helper()
	ctx := context.Background()

	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	var keys []string
	for _, obj := range out.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys
}

// getObject downloads an object from MinIO.
func getObject(t *testing.T, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	ctx := context.Background()

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	return data
}

func TestScenario_S3(t *testing.T) {
	connStr := startPostgres(t)
	channel := createTrigger(t, connStr, "s3_events")
	endpoint := startMinIO(t)
	minioClient := newMinIOClient(t, endpoint)

	t.Run("happy path jsonl", func(t *testing.T) {
		bucket := "test-jsonl"
		createBucket(t, minioClient, bucket)

		a := s3adapter.New(
			bucket,
			"cdc/",
			endpoint,
			"us-east-1",
			"minioadmin",
			"minioadmin",
			"jsonl",
			1*time.Second, // flush interval (fast for test)
			100,           // flush size
			30*time.Second,
			0, 0,
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)
		time.Sleep(500 * time.Millisecond)

		// Insert 3 rows.
		for i := range 3 {
			insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
		}

		// Wait for flush (interval=1s + margin).
		time.Sleep(3 * time.Second)

		// List objects in bucket.
		keys := listObjects(t, minioClient, bucket, "cdc/")
		if len(keys) == 0 {
			t.Fatal("expected at least one object in S3")
		}

		// Verify channel partitioning in key.
		found := false
		for _, key := range keys {
			if strings.Contains(key, "channel=pgcdc_s3_events") && strings.Contains(key, ".jsonl") {
				found = true

				// Download and verify content is valid JSON Lines.
				data := getObject(t, minioClient, bucket, key)
				scanner := bufio.NewScanner(bytes.NewReader(data))
				var lineCount int
				for scanner.Scan() {
					lineCount++
					var ev map[string]any
					if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
						t.Fatalf("line %d is not valid JSON: %v", lineCount, err)
					}
					if _, ok := ev["id"]; !ok {
						t.Fatalf("line %d missing 'id' field", lineCount)
					}
					if _, ok := ev["channel"]; !ok {
						t.Fatalf("line %d missing 'channel' field", lineCount)
					}
				}
				if lineCount != 3 {
					t.Fatalf("expected 3 JSON lines, got %d", lineCount)
				}
				break
			}
		}
		if !found {
			t.Fatalf("no object with expected channel partition and .jsonl extension found in keys: %v", keys)
		}
	})

	t.Run("parquet format", func(t *testing.T) {
		bucket := "test-parquet"
		createBucket(t, minioClient, bucket)

		a := s3adapter.New(
			bucket,
			"cdc/",
			endpoint,
			"us-east-1",
			"minioadmin",
			"minioadmin",
			"parquet",
			1*time.Second,
			100,
			30*time.Second,
			0, 0,
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)
		time.Sleep(500 * time.Millisecond)

		// Insert 3 rows.
		for i := range 3 {
			insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
		}

		// Wait for flush.
		time.Sleep(3 * time.Second)

		keys := listObjects(t, minioClient, bucket, "cdc/")
		if len(keys) == 0 {
			t.Fatal("expected at least one object in S3")
		}

		// Find the .parquet file.
		found := false
		for _, key := range keys {
			if strings.Contains(key, "channel=pgcdc_s3_events") && strings.Contains(key, ".parquet") {
				found = true

				// Download and verify it's valid Parquet.
				data := getObject(t, minioClient, bucket, key)
				pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatalf("open parquet: %v", err)
				}
				if pf.NumRows() != 3 {
					t.Fatalf("expected 3 parquet rows, got %d", pf.NumRows())
				}
				break
			}
		}
		if !found {
			t.Fatalf("no .parquet object found in keys: %v", keys)
		}
	})
}
