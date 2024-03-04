import random

def gera_cnpj():
    def calcula_digito_verificador(cnpj):
        def calc_digito(cnpj, pos):
            soma = 0
            for i in range(len(cnpj)):
                soma += int(cnpj[i]) * pos
                pos -= 1
                if pos < 2:
                    pos = 9
            digito = 11 - (soma % 11)
            return 0 if digito >= 10 else digito

        cnpj = [int(digit) for digit in cnpj]
        dv1 = calc_digito(cnpj[:12], 5)
        dv2 = calc_digito(cnpj[:12] + [dv1], 6)
        return cnpj + [dv1, dv2]

    cnpj_base = [random.randint(0, 9) for _ in range(8)] + [0, 0, 0, 1]
    cnpj_com_dvs = calcula_digito_verificador(cnpj_base)
    return ''.join(map(str, cnpj_com_dvs))

# Gerar 10 CNPJs válidos
for _ in range(30):
    cnpj = gera_cnpj()
    print(cnpj)
