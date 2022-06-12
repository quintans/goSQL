package dbx

import (
	"unicode"
)

// converts helloWorld -> HELLO_WORLD
func FromCamelCase(name string) string {
	str := ""
	if name != "" {
		for _, letter := range name {
			if !unicode.IsLower(letter) {
				str += "_"
			}
			str += string(unicode.ToUpper(letter))
		}
	}

	return str
}

func ToCamelCase(name string) string {
	str := ""
	if name != "" {
		upper := true
		for _, letter := range name {
			if string(letter) == "_" {
				upper = true
			} else {
				if upper {
					str += string(unicode.ToUpper(letter))
					upper = false
				} else {
					str += string(unicode.ToLower(letter))
				}
			}
		}
	}

	return str
}
