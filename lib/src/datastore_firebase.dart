import 'dart:typed_data';
import 'package:firebase_storage/firebase_storage.dart';
import 'package:kiss_datastore/kiss_datastore.dart';

class DatastoreFirebase implements Datastore {
  DatastoreFirebase(this.store);
  final FirebaseStorage store;

  @override
  String get providerName => 'firebase';

  @override
  Upload<DatastoreItem> putData(
    String path,
    Uint8List data, {
    String? contentType,
    String? contentEncoding,
    String? contentLanguage,
    String? cacheControl,
    void Function(DatastoreItem)? onComplete,
  }) {
    final metadata = SettableMetadata(
      cacheControl: cacheControl ?? 'public, max-age=86400 ',
      contentType: contentType,
      contentEncoding: contentEncoding,
    );
    final ref = store.ref(path);
    final task = ref.putData(data, metadata);
    final resultFuture = task.then((task) async {
      final url = await task.ref.getDownloadURL();
      final item = DatastoreItem(
        uri: Uri.parse(url),
        contentType: contentType ?? 'application/octet-stream',
        uploadDate: DateTime.now(),
        providerName: providerName,
        prividerIdentifier: ref.fullPath,
      );
      onComplete?.call(item);
      return item;
    });
    return Upload(
      task,
      task.snapshotEvents.map((event) {
        if (event.totalBytes == 0) {
          return 0;
        }
        if (event.bytesTransferred == 0) {
          return 0;
        }
        final progress = (event.bytesTransferred / event.totalBytes) * 100;
        return progress.toInt();
      }),
      resultFuture,
      task.cancel,
      task.pause,
      task.resume,
      ref.fullPath,
      contentType,
    );
  }

  @override
  Future<bool> exists(String path) async {
    return store
        .ref(path)
        .getMetadata()
        .then((value) => true)
        .catchError((error) => false);
  }

  @override
  Future<Uri> getDownloadLink(String path, {DateTime? expires}) async {
    if (expires != null) {
      throw UnimplementedError('expires is not supported');
    }
    final url = await store.ref(path).getDownloadURL();
    return Uri.parse(url);
  }

  @override
  Future<void> delete(String path) => store.ref(path).delete();

  @override
  Future<DatastoreItem> get(String path) => store
      .ref(path)
      .getMetadata()
      .then(
        (value) => DatastoreItem(
          uri: Uri.parse(value.fullPath),
          contentType: value.contentType ?? 'application/octet-stream',
          uploadDate: value.timeCreated ?? DateTime.now(),
          providerName: providerName,
          prividerIdentifier: value.fullPath,
        ),
      );
}
