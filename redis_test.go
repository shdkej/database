package database

import (
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

	Context("Test sets", func() {
		tag := Note{
			FileName: "main.md",
			Tag:      "Good",
			TagLine:  "Good Enough",
		}
		tagPrefix := "tag:"

		It("set Sets", func() {
			Expect(pool.Create(tagPrefix, tag)).Should(BeNil())
		})
		It("get Sets", func() {
			Expect(pool.Get(tagPrefix + tag.Tag)).Should(Equal(tag))
		})
		It("get empty Sets", func() {
			Expect(pool.Get("empty")).Should(Equal(Note{}))
		})
		It("delete Sets", func() {
			Expect(pool.Delete(tag.Tag)).Should(BeNil())
		})
	})

	Context("Test Misc Function", func() {
		tag := Note{
			FileName: "main.md",
			Tag:      "Good",
			TagLine:  "Good Enough",
		}
		tagPrefix := "tag:"
		pool.Create(tagPrefix, tag)
		tags, err := pool.Scan(tagPrefix)
		It("get scan body", func() {
			Expect(tags).NotTo(BeNil())
		})
		It("check error", func() {
			Expect(err).Should(BeNil())
		})
	})
})
