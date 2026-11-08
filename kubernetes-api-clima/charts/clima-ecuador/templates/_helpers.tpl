{{- define "clima.nombre" -}}clima-ecuador{{- end }}
{{- define "clima.etiquetas" -}}
app.kubernetes.io/name: {{ include "clima.nombre" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

