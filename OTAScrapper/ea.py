import os

def translate_text(target, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    import six
    from google.cloud import translate_v2 as translate

    translate_client = translate.Client()

    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    result = translate_client.translate(text, target_language=target, format_='text')

    print(u"Text: {}".format(result["input"]))
    print(repr("Translation: {}".format(result["translatedText"])))
    print(u"Detected source language: {}".format(result["detectedSourceLanguage"]))


def _detect_language(text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    import six
    from google.cloud import translate_v2 as translate

    translate_client = translate.Client()

    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    result = translate_client.detect_language(text)
    return result

import os
from google.cloud import translate_v2 as translate

def detect_language(text):
    translate_client = translate.Client()
    result = translate_client.detect_language(text)
    return result

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './skripsi-hasea-9494d44e84dd.json'
print(detect_language('Hello World!'))

