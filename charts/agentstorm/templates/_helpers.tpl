{{- define "agentstorm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agentstorm.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "agentstorm.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "agentstorm.labels" -}}
app.kubernetes.io/name: {{ include "agentstorm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: agentstorm
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "agentstorm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "agentstorm.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "agentstorm.image" -}}
{{- $image := index . 0 -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $image.repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image.repository (required "image.tag is required when image.digest is empty" $image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "agentstorm.resultApiName" -}}
{{- printf "%s-result-api" (include "agentstorm.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agentstorm.resultSinkEnabled" -}}
{{- if or .Values.controller.resultSink.enabled .Values.resultApi.enabled -}}true{{- end -}}
{{- end -}}

{{- define "agentstorm.resultSinkURL" -}}
{{- if .Values.resultApi.enabled -}}
{{- printf "http://%s.%s.svc.cluster.local:8080" (include "agentstorm.resultApiName" .) .Release.Namespace -}}
{{- else -}}
{{- required "controller.resultSink.url is required when the external result sink is enabled" .Values.controller.resultSink.url -}}
{{- end -}}
{{- end -}}

{{- define "agentstorm.authSecret" -}}
{{- if .Values.resultApi.enabled -}}
{{- required "resultApi.existingAuthSecret is required when resultApi.enabled=true" .Values.resultApi.existingAuthSecret -}}
{{- else -}}
{{- required "controller.resultSink.existingAuthSecret is required when the result sink is enabled" .Values.controller.resultSink.existingAuthSecret -}}
{{- end -}}
{{- end -}}
