package nebulagolang

import (
	"errors"
	"fmt"
	"github.com/thalesfu/nebulagolang/basictype"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	"log"
)

type NebulaDB struct {
	account *Account
	spaces  map[string]*Space
}

func LoadDB() (*NebulaDB, bool) {
	account, ok := LoadAccount()

	if !ok {
		return nil, false
	}

	return &NebulaDB{
		account: account,
		spaces:  make(map[string]*Space),
	}, true
}

func (db *NebulaDB) Execute(stmt string) (*nebulago.ResultSet, bool, error) {
	var logger = nebulago.DefaultLogger{}
	hostAddress := nebulago.HostAddress{Host: db.account.Host, Port: db.account.Port}
	hostList := []nebulago.HostAddress{hostAddress}
	// Create configs for connection pool using default values
	testPoolConfig := nebulago.GetDefaultConf()

	// Initialize connection pool
	pool, err := nebulago.NewConnectionPool(hostList, testPoolConfig, logger)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d, %s", db.account.Host, db.account.Port, err.Error()))
	}
	// Close all connections in the pool
	defer pool.Close()

	// Create session
	session, err := pool.GetSession(db.account.Username, db.account.Password)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			db.account.Username, db.account.Password, err.Error()))
	}
	// Release session and return connection back to connection pool
	defer session.Release()

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
