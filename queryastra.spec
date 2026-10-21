# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for queryastra backend (Python 3.12)
# Build: uv run pyinstaller queryastra.spec --clean

block_cipher = None

a = Analysis(
    ['_entry.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('app/assets', 'app/assets'),
        ('migrations', 'migrations'),
        ('alembic.ini', '.'),
    ],
    hiddenimports=[
        # asyncpg C extension pieces PyInstaller misses
        'asyncpg',
        'asyncpg.pgproto',
        'asyncpg.pgproto.pgproto',
        'asyncpg.protocol',
        'asyncpg.protocol.protocol',
        'asyncpg.connection',

        # psycopg3
        'psycopg',
        'psycopg.adapt',
        'psycopg.pq',
        'psycopg.pq._pq_ctypes',
        'psycopg._pq',
        'psycopg.types',
        'psycopg.types.numeric',
        'psycopg.types.text',
        'psycopg.types.datetime',
        'psycopg.types.extra',
        'psycopg_binary',

        # aiomysql (pure Python, but safe to list)
        'aiomysql',
        'pymysql',
        'pymysql.connections',
        'pymysql.converters',

        # SQL Server drivers
        'pyodbc',
        'pymssql',

        # Oracle (thin mode — no Oracle Client needed)
        'oracledb',
        'oracledb.impl',
        'oracledb.impl.python',
        'oracledb.impl.python.connect_params',
        'oracledb.impl.python.pool_params',

        # SQLAlchemy dialects auto-loaded by engine URL at runtime
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy.dialects.postgresql.asyncpg',
        'sqlalchemy.dialects.postgresql.psycopg',
        'sqlalchemy.dialects.mysql',
        'sqlalchemy.dialects.mysql.aiomysql',
        'sqlalchemy.dialects.mssql',
        'sqlalchemy.dialects.mssql.pyodbc',
        'sqlalchemy.dialects.mssql.pymssql',
        'sqlalchemy.dialects.oracle',
        'sqlalchemy.dialects.oracle.oracledb',
        'sqlalchemy.ext.asyncio',

        # Auth
        'passlib',
        'passlib.handlers',
        'passlib.handlers.bcrypt',
        'passlib.handlers.sha2_crypt',
        'bcrypt',
        'jose',
        'jose.jwt',
        'jose.jwk',
        'jose.jws',

        # Crypto
        'cryptography',
        'cryptography.hazmat.primitives',
        'cryptography.hazmat.backends',
        'cryptography.fernet',

        # Reporting / scheduling
        'reportlab',
        'reportlab.pdfgen',
        'reportlab.lib',
        'reportlab.platypus',
        'croniter',

        # Alembic (used at runtime for migrations)
        'alembic',
        'alembic.config',
        'alembic.env',
        'alembic.runtime',
        'alembic.runtime.migration',
        'alembic.operations',

        # uvicorn internals PyInstaller misses
        'uvicorn',
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.asyncio',
        'uvicorn.loops.uvloop',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.http.h11_impl',
        'uvicorn.protocols.http.httptools_impl',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.protocols.websockets.websockets_impl',
        'uvicorn.protocols.websockets.wsproto_impl',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',

        # Networking
        'websockets',
        'httpx',
        'httpx._transports',
        'httpx._transports.default',
        'h11',
        'anyio',
        'anyio._backends._asyncio',
        'sniffio',

        # pydantic v2
        'pydantic',
        'pydantic_core',
        'pydantic_settings',

        # Python standard-lib pieces sometimes missed on Windows
        'email',
        'email.mime',
        'email.mime.text',
        'email.mime.multipart',
        'smtplib',
        'multiprocessing.freeze_support',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # Strip test, notebook, and GUI packages to reduce binary size
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'PIL',
        'IPython',
        'jupyter',
        'notebook',
        'pytest',
        'mypy',
        'ruff',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='queryastra',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=['vcruntime140.dll', 'python3*.dll'],
    runtime_tmpdir=None,
    console=True,
    icon=None,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
