# protocol.py
"""Protocolo de mensagens entre cliente e servidor.

Toda mensagem vai com um cabeçalho de 8 bytes contendo o tamanho do corpo
(framing por length-prefix). Isso elimina a dependência de fechar a conexão
para delimitar a mensagem, permite mensagens grandes com segurança e deixa o
protocolo robusto o suficiente para medir tempos de forma confiável.
"""
import pickle
import struct

# "!Q" = inteiro de 8 bytes, big-endian (network byte order).
_CABECALHO = struct.Struct("!Q")
_CHUNK = 1 << 20  # lê/escreve em blocos de 1 MiB


def enviar_msg(sock, objeto):
    """Serializa o objeto e envia precedido do seu tamanho."""
    corpo = pickle.dumps(objeto, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(_CABECALHO.pack(len(corpo)) + corpo)


def _receber_exato(sock, n):
    """Lê exatamente n bytes do socket ou levanta erro se a conexão cair."""
    buffer = bytearray()
    while len(buffer) < n:
        pedaco = sock.recv(min(n - len(buffer), _CHUNK))
        if not pedaco:
            raise ConnectionError(
                "conexão encerrada antes da mensagem completa "
                f"({len(buffer)}/{n} bytes recebidos)"
            )
        buffer.extend(pedaco)
    return bytes(buffer)


def receber_msg(sock):
    """Lê uma mensagem completa (cabeçalho + corpo) e devolve o objeto."""
    cabecalho = _receber_exato(sock, _CABECALHO.size)
    (tamanho,) = _CABECALHO.unpack(cabecalho)
    return pickle.loads(_receber_exato(sock, tamanho))
