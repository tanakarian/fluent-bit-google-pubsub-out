package main

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"strings"
)

type flbFormat struct {
	TimestampFormat string
}

func (f *flbFormat) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer

	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	b.WriteString("[")
	b.WriteString(entry.Time.Format(f.TimestampFormat))
	b.WriteString("] ")
	b.WriteByte('[')
	b.WriteString(strings.ToLower(entry.Level.String()))
	b.WriteString("] ")

	b.WriteByte('[')
	b.WriteString("out.gcloud_pubsub")
	b.WriteString("] ")

	if entry.Message != "" {
		b.WriteString(entry.Message)
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}
