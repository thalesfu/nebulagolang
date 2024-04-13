package nebulagolang

import (
	"errors"
	"fmt"
	"github.com/thalesfu/nebulagolang/basictype"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	"log"
	"strings"
)

type NebulaDB struct {
	account *Account
	spaces  map[string]*Space
	pool    *nebulago.ConnectionPool
}

func (db *NebulaDB) Close() {
	db.pool.Close()
}

func LoadDB() (*NebulaDB, bool) {
	account, ok := LoadAccount()

	if !ok {
		return nil, false
	}

	var logger = nebulago.DefaultLogger{}
	hostAddress := nebulago.HostAddress{Host: account.Host, Port: account.Port}
	hostList := []nebulago.HostAddress{hostAddress}
	// Create configs for connection pool using default values
	testPoolConfig := nebulago.GetDefaultConf()
	testPoolConfig.MinConnPoolSize = 50

	// Initialize connection pool
	pool, err := nebulago.NewConnectionPool(hostList, testPoolConfig, logger)

	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d, %s", account.Host, account.Port, err.Error()))
	}

	return &NebulaDB{
		account: account,
		spaces:  make(map[string]*Space),
		pool:    pool,
	}, true
}

func (db *NebulaDB) Execute(stmts ...string) (*nebulago.ResultSet, bool, error) {
	// Create session
	session, err := db.pool.GetSession(db.account.Username, db.account.Password)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			db.account.Username, db.account.Password, err.Error()))
	}
	// Release session and return connection back to connection pool
	defer session.Release()

	for i, s := range stmts {
		stmts[i] = s + ";"
	}

	stmt := strings.Join(stmts, "")

	resultSet, err := session.Execute(stmt)

	if err != nil {
		return nil, false, errors.New(fmt.Sprintf("throw error: \"%s\" when execute the statement: \"%s\"", err.Error(), stmt))
	}

	if !resultSet.IsSucceed() {
		return resultSet, false, errors.New(fmt.Sprintf("throw error: \"%s\" when execute the statement: \"%s\"", resultSet.GetErrorMsg(), stmt))
	}

	return resultSet, true, nil
}

func (db *NebulaDB) Use(space string) *Space {
	if sp, ok := db.spaces[space]; ok {
		return sp
	}

	sp := &Space{
		Name:   space,
		Nebula: db,
	}

	db.spaces[space] = sp

	return sp
}

func (db *NebulaDB) CreateSpace(space string, vidType basictype.BasicType, partitionNum int, replicaFactor int) (*nebulago.ResultSet, bool, error) {
	stmt := fmt.Sprintf("CREATE SPACE IF NOT EXISTS %s(partition_num=%d, replica_factor=%d, vid_type=%s);", space, partitionNum, replicaFactor, vidType.String())

	return db.Execute(stmt)
}

func (db *NebulaDB) ShowSpaces() (*nebulago.ResultSet, bool, error) {
	return db.Execute("Show Spaces;")
}
