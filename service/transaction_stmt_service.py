import ydb

from borb.pdf import Document, Barcode, BarcodeType, Table, FixedColumnWidthTable, Alignment
from borb.pdf import Page
from borb.pdf import SingleColumnLayout
from borb.pdf import Paragraph
from borb.pdf import PDF
from borb.io.read.types import Decimal

from db.repository.agreement_repository import AgreementRepository
from db.repository.client_repository import ClientRepository
from db.repository.transaction_stmt_repository import TransactionStmtRepository


class TransactionStmtService:
    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

        self.__transactionStmtRepo = TransactionStmtRepository()
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)

    @staticmethod
    def build_client_fio(client):
        if len(client) == 0:
            return "-"
        name = client[0].get('name', '')
        surname = client[0].get('surname', '')
        patronymic = client[0].get('patronymic', '')
        patronymic_short = ''
        if len(patronymic) > 0:
            patronymic_short = patronymic[0] + "."

        return name + " " + surname + " " + patronymic_short

    def save_stmt(self, txn):
        receiver = self.__clientRepo.find_by_agreement_id(txn['receiver_agreement_id'])
        receiver_phone = (receiver[0] if len(receiver) > 0 else {}).get('phone', '-')
        receiver_fio = self.build_client_fio(receiver)
        originator = self.__clientRepo.find_by_agreement_id(txn['originator_agreement_id'])
        originator_phone = (originator[0] if len(originator) > 0 else {}).get('phone', '-')
        originator_fio = self.build_client_fio(originator)
        document = Document()
        page = Page()
        layout = SingleColumnLayout(page)

        layout.add(Paragraph("Paycheck", font_size=Decimal(20)))

        layout.add(Barcode(data=txn['id'], type=BarcodeType.QR, width=Decimal(64), height=Decimal(64)))
        layout.add(Paragraph(""))

        layout.add(
            FixedColumnWidthTable(
                number_of_columns=2,
                number_of_rows=8,
                column_widths=[Decimal(50), Decimal(50)],
            )
            .add(Paragraph('Operation date'))
            .add(Paragraph(txn['transaction_date'], horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('Operation amount'))
            .add(Paragraph("{:.2f} RUB".format(float(txn['transaction_amount'])), horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('ORN'))
            .add(Paragraph(txn['orn'], horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('RRN'))
            .add(Paragraph(txn['rrn'], horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('Receiver name'))
            .add(Paragraph(receiver_fio, horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('Receiver phone number'))
            .add(Paragraph(receiver_phone, horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('Originator name'))
            .add(Paragraph(originator_fio, horizontal_alignment=Alignment.RIGHT))
            .add(Paragraph('Originator phone number'))
            .add(Paragraph(originator_phone, horizontal_alignment=Alignment.RIGHT))
            .set_padding_on_all_cells(Decimal(3), Decimal(3), Decimal(3), Decimal(3))
            .no_borders()
        )

        document.add_page(page)

        with open("output.pdf", "wb") as pdf_file_handle:
            PDF.dumps(pdf_file_handle, document)

        self.__transactionStmtRepo.upload_file(file_path='output.pdf', key=txn['id'])
