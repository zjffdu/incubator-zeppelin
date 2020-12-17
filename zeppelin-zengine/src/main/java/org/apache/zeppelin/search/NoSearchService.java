package org.apache.zeppelin.search;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class NoSearchService extends SearchService {

  @Inject
  public NoSearchService(ZeppelinConfiguration conf) {
    super("NoSearchService-Thread");
  }

  public NoSearchService(String name) {
    super(name);
  }

  @Override
  public List<Map<String, String>> query(String queryStr) {
    return null;
  }

  @Override
  public void updateNoteIndexDoc(Note note) throws IOException {

  }

  @Override
  public void updateParagraphIndexDoc(Paragraph paragraph) throws IOException {

  }

  @Override
  public void addIndexDocs(Collection<Note> collection) {

  }

  @Override
  public void addIndexDoc(Note note) {

  }

  @Override
  public void deleteIndexDocs(String noteId) {

  }

  @Override
  public void deleteIndexDoc(String noteId, Paragraph p) {

  }

  @Override
  public void startRebuildIndex(Stream<Note> notes) {

  }
}
