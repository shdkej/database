package database

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	parsing "github.com/shdkej/note-server/parsing"
)

func TestLocal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Load from Data source Test")
}

var _ = Describe("Test Load Data", func() {
	Context("Test Initial", func() {
		taglines, err := parsing.GetTagAll()
		It("get tagline all", func() {
			Expect(len(taglines)).NotTo(BeZero())
			Expect(err).Should(BeNil())
		})
	})

	Context("Test with Redis", func() {
		redis := &Redis{}
		err := redis.Init()
		It("Test initial", func() {
			Expect(err).Should(BeNil())
		})

		v := DB{Store: redis, prefix: "tag:"}
		It("Test is exist initial content", func() {
			value, err := v.Get("#### kubernetes")
			Expect(value).NotTo(BeNil())
			Expect(err).Should(BeNil())
		})

		tag := Note{
			FileName: "main.md",
			Tag:      "Good",
			TagLine:  "Good Enough",
		}

		It("Test change table, first miss", func() {
			v.SetPrefix("test:")
			value, err := v.Get("Good")
			Expect(value).Should(Equal(Note{}))
			Expect(err).Should(BeNil())
		})

		It("Test change table, write and read", func() {
			v.Put(tag)
			value, err := v.Get("Good")
			Expect(value).NotTo(BeNil())
			Expect(value.TagLine).Should(Equal(tag.TagLine))
			Expect(err).Should(BeNil())
		})

		It("clean up", func() {
			Expect(v.Delete(tag.Tag)).Should(BeNil())
		})
	})

})
