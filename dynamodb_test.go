package database

import (
	"github.com/fatih/structs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Running Dynamodb", func() {
	Context("Test dynamodb CRUD", func() {
		conn := Dynamodb{}
		tableName := "myBlog"
		tag := Object{
			ID:      "123456",
			Name:    "Good",
			Content: "Good Enough",
		}
		mappedTag := structs.Map(tag)
		mappedTag["Tag"] = "test"
		m := make(map[string]string, len(mappedTag))
		for i, v := range mappedTag {
			m[i] = v.(string)
		}

		It("Init", func() {
			Expect(conn.Init()).Should(BeNil())
		})
		It("Get Table", func() {
			Expect(conn.getTable()).Should(BeNil())
			Expect(conn.TableName).Should(Equal(tableName))
		})
		It("Create Item", func() {
			Expect(conn.Create(mappedTag)).Should(BeNil())
		})
		It("Get Item", func() {
			Expect(conn.Get("test")).Should(Equal(m))
		})
		It("Scan Item", func() {
			result, err := conn.Scan("test")
			Expect(result).NotTo(BeZero())
			Expect(err).Should(BeNil())
			Expect(result[0]).NotTo(BeNil())
		})
		It("Update Item", func() {
			Expect(conn.Update(tag.Name, "Name", "test")).Should(BeNil())
		})
		It("Delete Item", func() {
			Expect(conn.Delete(tag.Name)).Should(BeNil())
		})
	})
})
