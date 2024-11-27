# covid-19

1. Solução Robusta para Problemas Reais:
Configuração de Banco de Dados: Foi utilizado o PostgreSQL, que é recomendado para ambientes de produção.
Gerenciamento de Contêineres: Processo continuo com execução automatica.
Permissões: processo respeitou todo o permissionamento do Irflow com o Linux fora e dentro dos contêineres.

3. Arquitetura Flexível e Escalável:
Uso da Arquitetura Medalhão: Fluxo de dados baseado nas camadas RAW, REFINED e TRUSTED, promovendo uma estrutura modular e escalável.
Separação de Camadas: Garantimos que cada etapa do pipeline trabalhe de forma autonoma com os dados na camada correta, preparando o ambiente para novos desafios.

4. Integração com Tecnologias Modernas:
Integração com Spark: Solução foi desenhada com Pyspark manipular grandes volumes de dados, indo além do uso básico do Airflow.
Relatório Final: Criação de um relatório detalhado e pronto para consumo em formato CSV.

5. Resolução de Desafios Operacionais:
Configuração do Airflow: Ajustes ao Airflow, desde a criação do banco de dados até a configuração do executor e dos usuários.
Autenticação e Acesso: Criação de usuários para acesso à interface web do airflow.

6. Ensino e Aprendizado ao Longo do Processo:
Processo estruturado de maneira que o ambiente possa ser reutilizado e expandido em projetos futuros.

Resumo do Valor Entregue:
Foi entregue um pipeline de dados completo, confiável e com padrões de produção.
Ambiente pronto para evolução, suportando novas fontes de dados ou outros relatórios.
Este processo pode ser uma base sólida para qualquer projeto de dados ou similar.
Conclusão: Criamos um ambiente robusto e escalável que pode servir de exemplo para projetos futuros

![image](https://github.com/user-attachments/assets/86963c26-31e6-4dab-9ba2-377bef5e012a)




