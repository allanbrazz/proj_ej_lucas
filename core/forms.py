from django import forms


class ServerSyncForm(forms.Form):
    nome = forms.CharField(
        max_length=150,
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Ex.: Busca do dia / Sincronização manual',
            }
        ),
    )
    filtro = forms.CharField(
        max_length=200,
        required=False,
        widget=forms.TextInput(
            attrs={
                'class': 'form-control',
                'placeholder': 'Filtro de busca opcional enviado ao servidor',
            }
        ),
    )
    data_inicio = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
    )
    data_fim = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
    )

    def clean(self):
        cleaned = super().clean()
        inicio = cleaned.get('data_inicio')
        fim = cleaned.get('data_fim')
        if inicio and fim and fim < inicio:
            raise forms.ValidationError('A data final não pode ser anterior à data inicial.')
        return cleaned
