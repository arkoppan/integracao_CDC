import logging
import azure.functions as func
import json
import pyodbc
import requests
import sys


def integracao_cdc(cpf, cpfInt, tokenType, token):
    try:
        logging.info("==Começo da função 'integracao_cdc', cpf: " + cpf)
        # Declaração Auxiliar - API Porto Seguro
        headersRequest = {'Content-Type': 'application/json', "Authorization": ""}
        payloadRequest = {"cpf": "", "preferences": [{"prefId": "marketingOptInFlag", "value": "false"}]}

        # Declaração de Variáveis
        comandoSQLParametros = """\
        SELECT TOP 1
        A.sqlBuscaCPFs
        , A.urlRequest
        , A.urlLogin
	    , A.client_id
	    , A.client_secret
        FROM [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros] A WITH(NOLOCK)
        """

        # Informações de Conexão
	## deleting the lines of code 29 to 33 here per customer request (Porto Seguro) as password is exposed 

        # Conecta no Banco de Dados
        conn = pyodbc.connect(stringConn)
        cursor = conn.cursor()
        conn.autocommit = False

        # Busca Parâmetros e CPFs
        parametros = cursor.execute(comandoSQLParametros).fetchone()

        # Gera o Json para enviar
        payloadRequest["cpf"] = str(cpf)
        jsonCPF = json.dumps(payloadRequest.copy())

        # Marca no Log o Json gerado
        sqlLog = """\
        SET NOCOUNT ON
        DECLARE @chaveUQ UNIQUEIDENTIFIER = NEWID()
        INSERT
        INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
        ( dataHoraInicio
        , idStatus
        , chaveUQ
        , jsonEnviado )
        SELECT dataHoraInicio = GETDATE()
        , idStatus       = 1
        , chaveUQ        = @chaveUQ
        , jsonEnviado    = '""" + str(jsonCPF) + """'
        SELECT chaveUQ = @chaveUQ
        SET NOCOUNT OFF
        """
        # Inicia o Log de Integração
        chaveUQ = cursor.execute(sqlLog).fetchone()
        cursor.commit()

        # Realiza o envio para o CDC
        headersRequest["Authorization"] = tokenType + " " + token
        parametros.urlRequest = parametros.urlRequest + cpf

        responseIntegracao = requests.put(parametros.urlRequest, headers=headersRequest,
                                          data=json.dumps(payloadRequest))

        # Verifica se a integração ocorreu com sucesso
        if responseIntegracao.status_code != 200:
            # Marca no Log o erro na integração
            sqlLog = """\
            SET NOCOUNT ON
            UPDATE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
            SET dataHoraFim  = GETDATE()
            , urlRequest = '""" + str(parametros.urlRequest) + """'
            , idStatus = 4
            , httpStatusCode = """ + str(responseIntegracao.status_code) + """
            WHERE chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
            SET NOCOUNT OFF
            """
            cursor.execute(sqlLog)
            cursor.commit()

            # Insere na tabela de erros
            sqlLog = """\
            SET NOCOUNT ON
            INSERT
            INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
            ( chaveUQ
            , msg )
            SELECT chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
            , msg = 'Ocorreu um erro durante a requisição HTTP. Status-Code - """ + str(responseIntegracao.status_code) \
            + """. Content: """ + str(responseIntegracao.content).replace("\'", "|") + """'
            SET NOCOUNT OFF
            """
            cursor.execute(sqlLog)
            cursor.commit()

        else:
            # Marca no Log o sucesso no envio
            sqlLog = """\
            SET NOCOUNT ON
            UPDATE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
            SET jsonRecebido = '""" + str(responseIntegracao.content).replace("\'", "|") + """'
            , urlRequest = '""" + parametros.urlRequest + """'
            , httpStatusCode = """ + str(responseIntegracao.status_code) + """
            WHERE chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
            SET NOCOUNT OFF
            """
            cursor.execute(sqlLog)
            cursor.commit()

            # Insere CPFs enviados na tabela de controle
            sqlLog = """\
            SET NOCOUNT ON
            INSERT
            INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_controle]
            ( num_docto       
            , chaveUQ           
            , dataHoraInsercao )
            SELECT num_docto = ?
            , chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
            , dataHoraInsercao = GETDATE()
            SET NOCOUNT OFF
            """
            try:
                cursor.execute(sqlLog, cpfInt)
            except:
                try:
                    cursor.rollback()
                except:
                    pass
                finally:
                    # Marca no Log o erro na integração
                    sqlLog = """\
                    SET NOCOUNT ON
                    UPDATE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
                    SET dataHoraFim  = GETDATE()
                    , idStatus = 3
                    WHERE chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
                    SET NOCOUNT OFF
                    """
                    cursor.execute(sqlLog)
                    cursor.commit()

                    # Insere na tabela de erros
                    sqlLog = """\
                    SET NOCOUNT ON
                    INSERT
                    INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
                    ( chaveUQ
                    , msg )
                    SELECT chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
                    , msg = '[Ocorreu um erro durante a inserção dos CPFs na tabela de controle] - """ + str(
                        sys.exc_info()).replace("\'", "\"") + """'
                    SET NOCOUNT OFF
                    """
                    cursor.execute(sqlLog)
                    cursor.commit()
            else:
                cursor.commit()

                # Marca no Log o sucesso na integração
                sqlLog = """\
                SET NOCOUNT ON
                UPDATE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
                SET dataHoraFim  = GETDATE()
                , idStatus = 2
                WHERE chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
                SET NOCOUNT OFF
                """
                cursor.execute(sqlLog)
                cursor.commit()

    except:
        # Insere no log caso haja algum erro durante a execução do script
        sqlLog = """\
        SET NOCOUNT ON
        UPDATE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
        SET dataHoraFim  = GETDATE()
        , idStatus = 3
        WHERE chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
        SET NOCOUNT OFF
        """
        cursor.execute(sqlLog)
        cursor.commit()

        # Insere na tabela de erros
        sqlLog = """\
        SET NOCOUNT ON
        INSERT
        INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
        ( chaveUQ
        , msg )
        SELECT chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
        , msg = '[Ocorreu um erro no script] - """ + str(sys.exc_info()).replace("\'", "\"") + """'
        SET NOCOUNT OFF
        """
        cursor.execute(sqlLog)
        cursor.commit()

    finally:
        try:
            try:
                cursor.close()
            except:
                pass
            conn.close()
        except:
            pass


def principal():
    # Log de Aplicação
    logging.info("Começo da função 'Principal'")

    # Declaração de Variáveis
    comandoSQLParametros = """\
            SELECT TOP 1
            A.sqlBuscaCPFs
            , A.urlLogin
    	    , A.client_id
    	    , A.client_secret
            FROM [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros] A WITH(NOLOCK)
            """
    headersLogin = {'Content-Type': 'application/x-www-form-urlencoded'}
    payloadLogin = {'client_id': "", "client_secret": "", "grant_type": "client_credentials"}

    # Informações de Conexão
##deleting the lines of code 236 to  240 here per customer request (Porto Seguro) as password is exposed 
 
    try:
        # Conecta no Banco de Dados
        conn = pyodbc.connect(stringConn)
        cursor = conn.cursor()
        conn.autocommit = False

        # Busca Parâmetros e CPFs
        parametros = cursor.execute(comandoSQLParametros).fetchone()
        rows = cursor.execute(parametros.sqlBuscaCPFs).fetchall()

        # Autentica na API da Porto Seguro
        payloadLogin["client_id"] = parametros.client_id
        payloadLogin["client_secret"] = parametros.client_secret

        responseLogin = requests.post(parametros.urlLogin, headers=headersLogin, data=payloadLogin)

        # Verifica se existem CPFs para envio
        if len(rows) > 0:
            for row in rows:
                # Executa a integração
                integracao_cdc(row.cpf, row.cpfInt, responseLogin.json()["token_type"], responseLogin.json()["access_token"])

        else:
            # Caso não haja, registra no log
            sqlLog = """\
            SET NOCOUNT ON
            INSERT
            INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
            ( dataHoraInicio
            , dataHoraFim
            , idStatus
            , chaveUQ )
            SELECT dataHoraInicio = GETDATE()
            , dataHoraFim	  = GETDATE()
            , idStatus		  = 5
            , chaveUQ		  = NEWID()
            SET NOCOUNT OFF
            """
            cursor.execute(sqlLog)
            cursor.commit()
            
    except:
        # Insere no log caso haja algum erro durante a execução do script
        sqlLog = """\
        SET NOCOUNT ON
        DECLARE @chaveUQ UNIQUEIDENTIFIER = NEWID()
        INSERT
        INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
        ( dataHoraInicio
        , dataHoraFim
        , idStatus
        , chaveUQ )
        SELECT dataHoraInicio = GETDATE()
        , dataHoraFim    = GETDATE()
        , idStatus       = 3
        , chaveUQ        = @chaveUQ
        SELECT chaveUQ = @chaveUQ
        SET NOCOUNT OFF
        """
        chaveUQ = cursor.execute(sqlLog).fetchone()
        cursor.commit()

        # Insere na tabela de erros
        sqlLog = """\
        SET NOCOUNT ON
        INSERT
        INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
        ( chaveUQ
        , msg )
        SELECT chaveUQ = '""" + str(chaveUQ.chaveUQ) + """'
        , msg = '[Ocorreu um erro no script] - """ + str(sys.exc_info()).replace("\'", "\"") + """'
        SET NOCOUNT OFF
        """
        cursor.execute(sqlLog)
        cursor.commit()

    finally:
        try:
            try:
                cursor.close()
            except:
                pass

            conn.close()
        except:
            pass


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Inicio da Trigger HTTP.')

    senha = req.params.get('senha')

    if senha:
        if senha == "Evan":
            principal()
            return func.HttpResponse(f"Parábens!")
        else:
            return func.HttpResponse(f"Por favor, informe a senha correta na requisição!")
    else:
        return func.HttpResponse(
             "Por favor, informe a senha na requisição.",
             status_code=400
        )
