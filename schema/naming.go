package schema

import (
	pluralizer "github.com/gertd/go-pluralize"
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func formatName(s string) string {
	if s == "ID" || s == "UUID" {
		return strings.ToLower(s)
	}
	snake := matchFirstCap.ReplaceAllString(s, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func pluralize(name string) string {
	client := pluralizer.NewClient()
	return strings.ToLower(client.Pluralize(name, 2, false))
}
