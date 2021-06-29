package database

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLocal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Load from Data source Test")
}

var _ = Describe("Test Load Data", func() {
	tag := Object{
		ID:      "12345",
		Name:    "Good",
		Content: "Good Enough",
	}
	tagPrefix := "test:"

	Context("Test with Redis", func() {
		redis := &Redis{}

		v := DB{Store: redis, prefix: "tag:"}
		err := v.Init()
		It("Test initial", func() {
			Expect(err).Should(BeNil())
		})
		It("Test is exist initial content", func() {
			value, err := v.Get("#### kubernetes")
			Expect(value).NotTo(BeNil())
			Expect(err).Should(BeNil())
		})

		It("Test change table, first miss", func() {
			v.SetPrefix("test")
			value, err := v.Get("Good")
			Expect(value).Should(Equal(Object{}))
			Expect(err).Should(BeNil())
		})

		It("Test change table, write and read", func() {
			v.Create(tag)
			value, err := v.Get("Good")
			Expect(value).NotTo(BeNil())
			Expect(value.Name).Should(Equal(tagPrefix + tag.Name))
			Expect(err).Should(BeNil())
		})

		It("clean up", func() {
			Expect(v.Delete(tag.Name)).Should(BeNil())
		})
	})

	Context("Test with DynamoDB", func() {
		dynamo := &Dynamodb{}

		v := DB{Store: dynamo, prefix: "tag:"}
		err := v.Init()
		It("Test initial", func() {
			Expect(err).Should(BeNil())
		})
		It("Test is exist initial content", func() {
			value, err := v.Get("#### kubernetes")
			Expect(value).NotTo(BeNil())
			Expect(err).Should(BeNil())
		})

		It("Test change table, first miss", func() {
			v.SetPrefix("test")
			value, err := v.Get("Good")
			Expect(value).Should(Equal(Object{}))
			Expect(err).Should(BeNil())
		})

		It("Test change table, write and read", func() {
			//v.Create(tag) TODO
			value, err := v.Get("Good")
			Expect(value).NotTo(BeNil())
			//Expect(value.Name).Should(Equal(tagPrefix + tag.Name))
			Expect(err).Should(BeNil())
		})

		It("clean up", func() {
			Expect(v.Delete(tag.Name)).Should(BeNil())
		})
	})

})
