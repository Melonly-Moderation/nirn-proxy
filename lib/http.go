package lib

import (
	"io"
	"net/http"
	"strings"
)

func copyHeader(dst, src http.Header) {
	dst["Date"] = nil
	dst["Content-Type"] = nil
	for k, vv := range src {
		for _, v := range vv {
			if k != "Content-Length" {
				dst[strings.ToLower(k)] = []string{v}
			}
		}
	}
}

func CopyResponseToResponseWriter(resp *http.Response, respWriter *http.ResponseWriter) error {
	writer := *respWriter
	defer resp.Body.Close()

	copyHeader(writer.Header(), resp.Header)
	writer.WriteHeader(resp.StatusCode)

	_, err := io.Copy(writer, resp.Body)
	return err
}
