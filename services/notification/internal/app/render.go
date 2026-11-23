package app

import (
	"bytes"
	"embed"
	"fmt"
	htmltemplate "html/template"
	"text/template"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

//go:embed templates/notifications.tmpl templates/email.html
var templateFS embed.FS

var (
	textTemplates = template.Must(template.New("").Option("missingkey=error").ParseFS(templateFS, "templates/notifications.tmpl"))
	emailTemplate = htmltemplate.Must(htmltemplate.ParseFS(templateFS, "templates/email.html"))
)

// fallbackJobTitle names a job whose replica row is missing.
const fallbackJobTitle = "tin tuyển dụng của bạn"

// statusLabels are the Vietnamese names of the recruiter-driven statuses.
var statusLabels = map[string]string{
	"INTERVIEW": "Mời phỏng vấn",
	"OFFERED":   "Đề nghị nhận việc",
	"HIRED":     "Đã được tuyển",
	"REJECTED":  "Chưa phù hợp",
}

// view is what the notification templates may read.
type view struct {
	JobTitle    string
	StatusLabel string
	Reason      string
	Count       int
}

// render fills title and body of a notification of type t.
func render(t domain.Type, v view) (title, body string, err error) {
	if title, err = execText(string(t)+".title", v); err != nil {
		return "", "", err
	}
	if body, err = execText(string(t)+".body", v); err != nil {
		return "", "", err
	}
	return title, body, nil
}

func execText(name string, v view) (string, error) {
	var b bytes.Buffer
	if err := textTemplates.ExecuteTemplate(&b, name, v); err != nil {
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	return b.String(), nil
}

// renderEmail wraps a notification in the HTML email layout (html/template
// escapes the text, e.g. a job title containing markup).
func renderEmail(title, body string) (string, error) {
	var b bytes.Buffer
	if err := emailTemplate.Execute(&b, struct{ Title, Body string }{title, body}); err != nil {
		return "", fmt.Errorf("render email: %w", err)
	}
	return b.String(), nil
}
