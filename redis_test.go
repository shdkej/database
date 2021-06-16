package database

import (
	"github.com/fatih/structs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Running Redis", func() {
	pool := Redis{}
	pool.Init()

	Context("Test ping", func() {
		It("pong", func() {
			Expect(pool.Ping()).Should(BeNil())
			Expect(pool.Hits("test")).NotTo(BeNil())
		})
	})

	tag := Object{
		ID:      "123456",
		Name:    "Good",
		Content: "Good Enough",
	}
	tagPrefix := "tag:"
	mappedTag := structs.Map(tag)
	m := make(map[string]string, len(mappedTag))
	for i, v := range mappedTag {
		m[i] = v.(string)
	}

	Context("Test sets", func() {
		It("set Sets", func() {
			Expect(pool.Create(mappedTag)).Should(BeNil())
		})
		It("get Sets", func() {
			Expect(pool.Get(tag.Name)).Should(Equal(m))
		})
		It("get empty Sets", func() {
			Expect(pool.Get("empty")).Should(Equal(map[string]string{}))
		})
		It("delete Sets", func() {
			Expect(pool.Delete(tag.Name)).Should(BeNil())
		})
	})

	Context("Test Misc Function", func() {
		pool.Create(mappedTag)
		tags, err := pool.Scan(tagPrefix)
		It("get scan body", func() {
			Expect(tags).NotTo(BeNil())
		})
		It("check error", func() {
			Expect(err).Should(BeNil())
		})
	})
})
