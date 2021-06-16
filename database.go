// data source from RDBMS, NoSQL
package database

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/fatih/structs"
)

// entrypoint use data source function
type DB struct {
	Store  DataSource
	prefix string
}

type Object struct {
	ID        string
	Name      string
	Content   string
	CreatedAt string
	UpdatedAt string
}

// implemented functions to use the data source
type DataSource interface {
	Init() error
	Ping() error
	Hits(string) (int64, error)
	Get(string) (map[string]string, error)
	Create(map[string]interface{}) error
	Update(string, string, interface{}) error
	Delete(string) error
	Scan(string) ([]string, error)
}

// for multiple table
func (v *DB) SetPrefix(prefix string) {
	var table string = os.Getenv("TABLE")
	if prefix != "" {
		table = prefix
	}
	if table == "" {
		table = "tag"
	}
	v.prefix = table + ":"
	log.Println(v.prefix)
}

// usage:
// db := database.DB{} // optional: prefix:"table"
// db.Init()
// data := `{"name":"new"}`
// db.Create(data)
// db.Get("new")
func (v *DB) Init() error {
	err := v.Store.Init()
	if err != nil {
		log.Println("init failed", err)
		return err
	}
	err = v.Store.Ping()
	if err != nil {
		log.Println("ping failed", err)
		return err
	}

	v.SetPrefix("")
	log.Println("Init Completed")
	return nil
}

func (v *DB) Hits(s string) int64 {
	hits, err := v.Store.Hits(s)
	if err != nil {
		log.Fatal(err)
		return hits
	}
	return hits
}

func (v *DB) GetEverything() ([]Object, error) {
	m, err := v.Store.Scan(v.prefix)
	if err != nil {
		return []Object{}, err
	}

	var notes []Object
	for _, value := range m {
		tag, err := v.Get(value)
		if err != nil {
			log.Println(err)
			return notes, err
		}
		notes = append(notes, tag)
	}

	return notes, nil
}

func (v *DB) Get(title string) (Object, error) {
	if title == "tag:" || title == "" {
		return Object{}, nil
	}

	if !strings.HasPrefix(title, v.prefix) {
		title = v.prefix + title
	}
	m, err := v.Store.Get(title)
	if err != nil {
		return Object{}, err
	}

	tag := Object{}
	for key, value := range m {
		switch key {
		case "ID":
			tag.ID = value
		case "Name":
			tag.Name = value
		case "Content":
			tag.Content = value
		case "CreatedAt":
			tag.CreatedAt = value
		case "UpdatedAt":
			tag.UpdatedAt = value
		}
	}
	return tag, nil
}

func (v *DB) Create(value Object) error {
	now := time.Now().Format("2006-01-02")
	value.CreatedAt = now
	value.UpdatedAt = now
	value.Name = v.prefix + value.Name
	note := structs.Map(value)
	err := v.Store.Create(note)
	if err != nil {
		return err
	}
	log.Println("put complete", value.Name)
	return nil
}

func (v *DB) Delete(key string) error {
	err := v.Store.Delete(v.prefix + key)
	if err != nil {
		return err
	}
	return nil
}

func (v *DB) Update(key string, tags interface{}) error {
	now := time.Now().Format("2006-01-02")
	err := v.Store.Update(v.prefix+key, "Tag", tags)
	err = v.Store.Update(v.prefix+key, "UpdatedAt", now)
	if err != nil {
		return err
	}
	return nil
}
