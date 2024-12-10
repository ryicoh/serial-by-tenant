package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

const uri = "postgres://ryicoh@localhost:5432/postgres?sslmode=disable"

func main() {
	// 2テナントに対して、1万ユーザーずつ 10並列でINSERTする
	N := 20000
	numWorkers := 10
	numTenants := 2

	setupDB(numTenants)

	queryFnCh, doneCh := setupWorkers(numWorkers)
	defer close(queryFnCh)

	for i := 0; i < N; i++ {
		i := i
		tenantId := i%numTenants + 1
		go func() {
			queryFnCh <- func(tx *sql.Tx) {
				// ロックを取るためだけに、user_tenant_locks を参照する
				// ここで指定した tenant_id の行のみに対してロックを取る
				// MAX(users.tenant_serial_id) で最後の tenant_serial_id を取得している。
				// 採番用テーブルにするなら、ここに最後の tenant_serial_id のカラムを追加するといい。
				unsafeExec(tx, fmt.Sprintf("SELECT 1 FROM user_tenant_locks WHERE tenant_id = %d FOR UPDATE", tenantId))

				row := unsafeQueryRow(tx, fmt.Sprintf("SELECT COALESCE(MAX(tenant_serial_id), 0) FROM users WHERE tenant_id = %d", tenantId))
				var maxSerialId int
				if err := row.Scan(&maxSerialId); err != nil {
					log.Fatal(err)
				}

				unsafeExec(tx, fmt.Sprintf("INSERT INTO users (tenant_id, tenant_serial_id, name) VALUES (%d, %d, 'user%d')", tenantId, maxSerialId+1, i))
			}
		}()
	}

	for i := 0; i < N; i++ {
		<-doneCh
	}
}

// ワーカーを起動して、クエリを実行する
// ワーカーごとに１つのDB接続を持つ
func setupWorkers(numWorkers int) (chan<- func(tx *sql.Tx), <-chan struct{}) {
	queryFnCh := make(chan func(tx *sql.Tx), numWorkers*10)
	doneCh := make(chan struct{}, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			db, err := sql.Open("postgres", uri)
			if err != nil {
				log.Fatal(err)
			}
			defer db.Close()

			for queryFn := range queryFnCh {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					log.Fatal(err)
				}
				queryFn(tx)
				if err := tx.Commit(); err != nil {
					log.Fatal(err)
				}
				doneCh <- struct{}{}
			}
		}()
	}

	return queryFnCh, doneCh
}

func setupDB(numTenants int) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	// テーブルを削除
	unsafeExec(tx, "DROP TABLE IF EXISTS users")
	unsafeExec(tx, "DROP TABLE IF EXISTS user_tenant_locks")
	unsafeExec(tx, "DROP TABLE IF EXISTS tenants")

	// テーブルを作成
	unsafeExec(tx, "CREATE TABLE tenants (id SERIAL PRIMARY KEY, name TEXT)")

	// テナントを作成
	for i := 1; i <= numTenants; i++ {
		unsafeExec(tx, fmt.Sprintf("INSERT INTO tenants (id, name) VALUES (%d, 'tenant%d')", i, i))
	}

	// テナントロックテーブルを作成
	unsafeExec(tx, "CREATE TABLE user_tenant_locks (tenant_id INTEGER PRIMARY KEY REFERENCES tenants(id))")
	for i := 1; i <= numTenants; i++ {
		unsafeExec(tx, fmt.Sprintf("INSERT INTO user_tenant_locks (tenant_id) VALUES (%d)", i))
	}

	// ユーザーテーブルを作成
	unsafeExec(tx, "CREATE TABLE users (id SERIAL PRIMARY KEY, tenant_id INTEGER REFERENCES tenants(id), tenant_serial_id INTEGER, name TEXT)")

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("DB setup complete")
}

func unsafeExec(tx *sql.Tx, query string) {
	_, err := tx.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}

func unsafeQueryRow(tx *sql.Tx, query string) *sql.Row {
	return tx.QueryRow(query)
}
